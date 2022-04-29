package app

import (
	jobv1 "k8s.io/api/batch/v1"
	jobinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/sample-controller/pkg/signals"
	"time"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	queueversioned "github.com/kube-queue/api/pkg/client/clientset/versioned"
	queueinformers "github.com/kube-queue/api/pkg/client/informers/externalversions"
	"github.com/kube-queue/job-extension/cmd/app/options"
	"github.com/kube-queue/job-extension/pkg/controller"
	"k8s.io/klog/v2"
)

// Run runs the server.
func Run(opt *options.ServerOption) error {
	var restConfig *rest.Config
	var err error
	// Set up signals so we handle the first shutdown signal gracefully.
	stopCh := signals.SetupSignalHandler()

	if restConfig, err = rest.InClusterConfig(); err != nil {
		if restConfig, err = clientcmd.BuildConfigFromFlags("", opt.KubeConfig); err != nil {
			return err
		}
	}

	k8sClientSet, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	queueClient, err := queueversioned.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	jobClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	queueInformerFactory := queueinformers.NewSharedInformerFactory(queueClient, 0)
	queueInformer := queueInformerFactory.Scheduling().V1alpha1().QueueUnits().Informer()

	jobInformerFactory := jobinformers.NewSharedInformerFactory(k8sClientSet, time.Second*30)
	jobInformer := jobInformerFactory.Batch().V1().Jobs().Informer()

	extensionController := controller.NewExtensionController(
		k8sClientSet,
		queueInformerFactory.Scheduling().V1alpha1().QueueUnits(),
		queueClient,
		jobInformerFactory.Batch().V1().Jobs(),
		jobClient)

	queueInformer.AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch qu := obj.(type) {
				case *v1alpha1.QueueUnit:
					if qu.Spec.ConsumerRef != nil &&
						qu.Spec.ConsumerRef.Kind == controller.ConsumerRefKind &&
						qu.Spec.ConsumerRef.APIVersion == controller.ConsumerRefAPIVersion {
						return true
					}
					return false
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    extensionController.AddQueueUnit,
				UpdateFunc: extensionController.UpdateQueueUnit,
				DeleteFunc: extensionController.DeleteQueueUnit,
			},
		},
	)

	jobInformer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			switch obj.(type) {
			case *jobv1.Job:
				return true
			default:
				return false
			}
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    extensionController.AddJob,
			UpdateFunc: extensionController.UpdateJob,
			DeleteFunc: extensionController.DeleteJob,
		},
	})
	// start queueunit informer
	go queueInformerFactory.Start(stopCh)
	// start job informer
	go jobInformerFactory.Start(stopCh)

	err = extensionController.Run(2, stopCh)
	if err != nil {
		klog.Fatalf("Error running tfExtensionController", err.Error())
		return err
	}

	return nil
}
