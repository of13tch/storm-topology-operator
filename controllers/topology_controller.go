/*
Copyright 2021.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"github.com/go-logr/logr"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"storm-topology-controller-configmaps/storm"
	"time"
)

// TopologyReconciler reconciles a Topology object
type TopologyReconciler struct {
	client.Client
	Log         logr.Logger
	Scheme      *runtime.Scheme
	StormClient storm.StormClient
}

// +kubebuilder:rbac:groups=storm.gresearch.co.uk,resources=topologies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storm.gresearch.co.uk,resources=topologies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=storm.gresearch.co.uk,resources=topologies/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Topology object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.7.0/pkg/reconcile
func (r *TopologyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("topology", req.NamespacedName)

	log.Info("Invoking Reconcile loop")

	topology := &apiv1.ConfigMap{}
	running := &apiv1.ConfigMap{}

	err := r.Get(ctx, req.NamespacedName, topology)
	runningErr := r.Get(ctx, types.NamespacedName{
		Namespace: req.NamespacedName.Namespace,
		Name:      req.NamespacedName.Name + ".status",
	}, running)

	// If configmap is not found, and there is a corrosponding .status configmap - we can assume this is a topology
	// that has been deleted, we should kill it in storm
	if err != nil && errors.IsNotFound(err) && runningErr == nil {
		log.Info("Stopping topology by name: " + req.Name)
		_, err := r.StormClient.KillTopologyByName(req.Name)
		if err != nil {
			log.Error(err, "Failed to stop topology")
			return ctrl.Result{}, err
		}

		err = r.Delete(ctx, running)
		if err != nil {
			log.Error(err, "Failed to delete running configmap")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// If the topology does not have storm.topology label, return and ignore this configmap object
	if topology.Labels["storm.topology"] != "true" {
		log.Info("The configmap is not meant for storm, return, none of our beeswax")
		return ctrl.Result{}, nil
	}

	// If we get here we are processing a configmap object that both exists, and has a label marking it as a topology
	// If the corrsponding .status configmap doesn't exist it probably hasn't been deployed yet
	// Deploy! (and create the .status configmap)
	if runningErr != nil && errors.IsNotFound(runningErr) {
		log.Info("The topology is not marked as deployed")
		// Define a new deployment
		job := storm.DeployStormJob(req.Name, topology)
		log.Info("Creating a new Job", "Deployment.Namespace", job.Namespace, "Deployment.Name", job.Name)
		err = r.Create(ctx, job)
		if err != nil {
			log.Error(err, "Failed to create Job")
			return ctrl.Result{RequeueAfter: 5.0 * time.Second, Requeue: true}, err
		}

		configMapData := make(map[string]string, 0)
		configMapData["image"] = topology.Data["image"]
		configMapData["args"] = topology.Data["args"]

		// Set observed Spec as desired state
		running = &apiv1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: req.NamespacedName.Namespace,
				Name:      req.NamespacedName.Name + ".status",
			},
			Data: configMapData,
		}

		err := r.Create(ctx, running)
		if err != nil {
			log.Error(err, "Failed to update topology deployed status")
			return ctrl.Result{}, err
		}

		// Deployment created successfully - return and requeue
		return ctrl.Result{RequeueAfter: time.Second * 5, Requeue: true}, nil
	}

	// If we made it all the way here, we have a topology, with a matching .status config that looks to be deployed
	// Let's check the Spec has not changed from the Status. If it has we will delete the status and requeue which
	// will redeploy (function above)
	if running.Data["image"] != topology.Data["image"] ||
		running.Data["args"] != topology.Data["args"] {
		log.Info("Topology detected Spec change, killing and marking for redeployment")
		_, err := r.StormClient.KillTopologyByName(req.Name)
		if err != nil {
			log.Error(err, "Failed to stop topology")
			return ctrl.Result{}, err
		}

		err = r.Delete(ctx, running)
		if err != nil {
			log.Error(err, "Failed to update topology spec configmap")
			return ctrl.Result{}, err
		}

		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopologyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apiv1.ConfigMap{}).
		Complete(r)
}
