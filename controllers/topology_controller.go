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
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	stormv1alpha1 "storm-topology-operator/api/v1alpha1"
	"storm-topology-operator/storm"
	"strconv"
	"time"
)

// TopologyManagerReconciler reconciles a TopologyManager object
type TopologyReconciler struct {
	client.Client
	Log             logr.Logger
	Scheme          *runtime.Scheme
	StormController storm.StormCluster
}

// +kubebuilder:rbac:groups=storm.gresearch.co.uk,resources=topologymanagers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storm.gresearch.co.uk,resources=topologymanagers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=storm.gresearch.co.uk,resources=topologymanagers/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the TopologyManager object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.7.0/pkg/reconcile
func (r *TopologyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("topology", req.NamespacedName)
	// Get Object Changed
	topology := &stormv1alpha1.Topology{}

	err := r.Get(ctx, req.NamespacedName, topology)
	if err != nil {
		if errors.IsNotFound(err) {
			// If deleted, delete topology
			log.Info("Stopping topology by name: " + req.Name)
			r.StormController.KillTopologyByName(req.Name)
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get TopologyManager")
		return ctrl.Result{}, err
	}

	if topology.Status.Deployed == nil {
		log.Info("The topology is not marked as deployed")
		// Define a new deployment
		job := r.jobStormTopology(req.Name, topology)
		log.Info("Creating a new Job", "Deployment.Namespace", job.Namespace, "Deployment.Name", job.Name)
		err = r.Create(ctx, job)
		if err != nil {
			log.Error(err, "Failed to create Job")
			return ctrl.Result{RequeueAfter: 5.0 * time.Second, Requeue: true}, err
		}

		// Set observed Spec as desired state
		deployed := true
		topology.Status.Deployed = &deployed

		topology.Status.Args = topology.Spec.Args
		topology.Status.Image = topology.Spec.Image

		err := r.Status().Update(ctx, topology)
		if err != nil {
			log.Error(err, "Failed to update topology deployed status")
			return ctrl.Result{}, err
		}

		// Deployment created successfully - return and requeue
		return ctrl.Result{RequeueAfter: time.Second * 5, Requeue: true}, nil
	}

	// If the observed status of the topology has changed from the spec, set Deployed as nil
	if topology.Status.Image != topology.Spec.Image ||
		!reflect.DeepEqual(topology.Status.Args, topology.Spec.Args) {
		log.Info("Topology detected Spec change, killing and marking for redeployment")
		r.StormController.KillTopologyByName(req.Name)
		topology.Status.Deployed = nil

		err := r.Status().Update(ctx, topology)
		if err != nil {
			log.Error(err, "Failed to update topology deployed status")
			return ctrl.Result{}, err
		}

		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{}, nil
}

func (r *TopologyReconciler) jobStormTopology(name string, m *stormv1alpha1.Topology) *batchv1.Job {
	BackoffLimit := int32(2)
	TTLSecondsAfterFinish := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-" + strconv.FormatInt(time.Now().Unix(), 10),
			Namespace: "default",
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &BackoffLimit,
			TTLSecondsAfterFinished: &TTLSecondsAfterFinish,
			Template: apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec{
					RestartPolicy: "Never",
					Containers: []apiv1.Container{
						{
							Name:  name,
							Image: m.Spec.Image,
							Args:  m.Spec.Args,
							Env: []apiv1.EnvVar{{
								Name:  "NIMBUS_SEEDS",
								Value: "[\"siembol-storm-nimbus\"]",
							}, {
								Name:  "TOPOLOGY_CLASS",
								Value: "uk.co.gresearch.siembol.parsers.storm.StormParsingApplication",
							}, {
								Name:  "TOPOLOGY_JAR",
								Value: "parsing-storm-1.72-SNAPSHOT.jar",
							},
							},
						},
					},
				},
			},
		},
	}
	return job
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopologyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&stormv1alpha1.Topology{}).
		Complete(r)
}
