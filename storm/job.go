package storm

import (
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	nimbusSeeds = "NIMBUS_SEEDS"
	sep         = "-c"
)

var (
	BackoffLimit          = int32(2)
	TTLSecondsAfterFinish = int32(0)
	RunAsUser             = int64(1000)
	executeMode           = int32(0744)
)

func makeJobMetaData(name string, namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name + "-" + strconv.FormatInt(time.Now().Unix(), 10),
		Namespace: namespace,
	}
}

func makeJobSpec(name string, m *apiv1.ConfigMap) batchv1.JobSpec {
	return batchv1.JobSpec{
		BackoffLimit:            &BackoffLimit,
		TTLSecondsAfterFinished: &TTLSecondsAfterFinish,
		Template: apiv1.PodTemplateSpec{
			Spec: apiv1.PodSpec{
				SecurityContext: &apiv1.PodSecurityContext{
					RunAsUser: &RunAsUser,
				},
				RestartPolicy: "Never",
				Containers: []apiv1.Container{
					{
						Name:  name,
						Image: m.Data["image"],
						Args:  strings.Split(m.Data["args"], " "),
						Env: []apiv1.EnvVar{{
							Name:  "NIMBUS_SEEDS",
							Value: os.Getenv(nimbusSeeds),
						}, {
							Name:  "TOPOLOGY_CLASS",
							Value: m.Data["class"],
						}, {
							Name:  "TOPOLOGY_JAR",
							Value: m.Data["jar"],
						},
						},
					},
				},
			},
		},
	}
}

func DeployStormJob(name string, m *apiv1.ConfigMap) *batchv1.Job {
	metadata := makeJobMetaData(name, m.Namespace)
	spec := makeJobSpec(name, m)

	if kerberosEnabled() {
		jobContainer := spec.Template.Spec.Containers[0]

		// We need to build a storm jar command with inject kerkberos params
		jobContainer.Command = []string{"storm"}
		args := append([]string{
			sep, "storm.thrift.transport=org.apache.storm.security.auth.kerberos.KerberosSaslTransportPlugin",
			sep, "java.security.auth.login.config=/tmp/jaas.conf",
			sep, "nimbus.seeds=$(NIMBUS_SEEDS)",
			sep, "nimbus.thrift.port=" + getEnvOrDefault("NIMBUS_PORT", "6627"),
			"jar", "$(TOPOLOGY_JAR)", "$(TOPOLOGY_CLASS)",
		}, strings.Split(m.Data["args"], " ")...)
		jobContainer.Args = args

		// Add volume as configmap with name mapped to env variable STORM_JAAS_CONFIGMAP
		spec.Template.Spec.Volumes = []apiv1.Volume{
			{
				Name: "storm-jaas",
				VolumeSource: apiv1.VolumeSource{
					ConfigMap: &apiv1.ConfigMapVolumeSource{
						LocalObjectReference: apiv1.LocalObjectReference{
							Name: getEnvOrDefault("STORM_JAAS_CONFIGMAP", "storm-jaas"),
						},
						DefaultMode: &executeMode,
					},
				},
			}, {
				Name: "storm-keytab",
				VolumeSource: apiv1.VolumeSource{
					Secret: &apiv1.SecretVolumeSource{
						SecretName: getEnvOrDefault("STORM_KEYTAB_SECRET", "storm-keytab"),
					},
				},
			},
		}

		// Mount jaas config to /tmp/jaas.conf
		jobContainer.VolumeMounts = []apiv1.VolumeMount{
			{
				Name:      "storm-jaas",
				MountPath: "/tmp/jaas.conf",
				SubPath:   "jaas.conf",
			},
			{
				Name:      "storm-keytab",
				MountPath: "/tmp/k.kt",
				SubPath:   "k.kt",
			},
		}

		spec.Template.Spec.Containers[0] = jobContainer
	}

	return &batchv1.Job{
		ObjectMeta: metadata,
		Spec:       spec,
	}

}

func getEnvOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
