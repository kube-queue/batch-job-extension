package controller

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	jobv1informer "k8s.io/client-go/informers/batch/v1"
	"k8s.io/client-go/kubernetes"
	jobclientset "k8s.io/client-go/kubernetes"
	"strings"
	"time"

	queueinformers "github.com/kube-queue/api/pkg/client/informers/externalversions/scheduling/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	queueversioned "github.com/kube-queue/api/pkg/client/clientset/versioned"
	jobv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	// MaxQueueRetries is the number of times a queue item will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the times
	// a queue item is going to be requeued:
	//
	// 1-10 retry times: 5ms, 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.3s, 2.6s,
	// 11-20 retry times: 5.1s, 10.2s, 20.4s, 41s, 82s, 164s, 328s, 656s(11min), 1312s(21min), 2624s(43min)
	MaxQueueRetries = 15
	// RetryInterval is the interval time when update tfjob failed
	RetryInterval    = 200 * time.Millisecond
	MaxUpdateRetries = 5
	// Suspend               = "suspend"
	Queuing               = "Queuing"
	ConsumerRefKind       = "Job"
	ConsumerRefAPIVersion = jobv1.GroupName + "/v1"
	// QuNameSuffix is the suffix of the queue unit name when create a new one.
	// In this way, different types of jobs with the same name will create different queue unit name.
	QuNameSuffix           = "-job-qu"
	OptimisticLockErrorMsg = "the object has been modified; please apply your changes to the latest version and try again"
)

type ExtensionController struct {
	k8sClient     *kubernetes.Clientset
	queueInformer queueinformers.QueueUnitInformer
	queueClient   *queueversioned.Clientset
	jobInformer   jobv1informer.JobInformer
	jobClient     *jobclientset.Clientset
	workqueue     workqueue.RateLimitingInterface
}

func NewExtensionController(
	k8sClient *kubernetes.Clientset,
	queueInformer queueinformers.QueueUnitInformer,
	queueClient *queueversioned.Clientset,
	jobInformer jobv1informer.JobInformer,
	jobClinet *jobclientset.Clientset) *ExtensionController {
	return &ExtensionController{
		k8sClient:     k8sClient,
		queueInformer: queueInformer,
		queueClient:   queueClient,
		jobInformer: jobInformer,
		jobClient:   jobClinet,
		workqueue:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "QueueUnit"),
	}
}

func (tc *ExtensionController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer tc.workqueue.ShutDown()

	klog.Info("Start ExtensionController Run function")
	if !cache.WaitForCacheSync(stopCh, tc.queueInformer.Informer().HasSynced) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	for i := 0; i < threadiness; i++ {
		go wait.Until(tc.runWorker, time.Second, stopCh)
	}
	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

func (tc *ExtensionController) runWorker() {
	for tc.processNextWorkItem() {
	}
}

func (tc *ExtensionController) processNextWorkItem() bool {
	obj, shutdown := tc.workqueue.Get()
	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer t.workqueue.Done.
	err := func(obj interface{}) error {
		defer tc.workqueue.Done(obj)
		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			tc.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		err := tc.syncHandler(key)
		tc.handleErr(err, key)

		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
	}

	return true
}

func (tc *ExtensionController) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return err
	}
	// Get queueunit from cache
	queueUnit, err := tc.queueInformer.Lister().QueueUnits(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Errorf("failed to find qu by:%v/%v, maybe qu has been deleted", namespace, name)
			// If can't get queueunit, return nil, handleErr function will forget key from workqueue
			return nil
		}
		runtime.HandleError(fmt.Errorf("failed to get queueunit by: %s/%s", namespace, name))

		return err
	}
	klog.Infof("Get informer from add/update event,queueUnit:%v/%v", queueUnit.Namespace, queueUnit.Name)

	if queueUnit.Status.Phase == v1alpha1.Dequeued {
		klog.Infof("QueueUnit %v/%v has dequeued", queueUnit.Namespace, queueUnit.Name)
		err = tc.deleteQueueAnnotationInJob(queueUnit)
		if errors.IsNotFound(err) {
			// If can't find tfjob for queueunit, return err, handleErr function will requeue key MaxRetries times
			return err
		}
	}

	return nil
}

func (tc *ExtensionController) deleteQueueUnitWhenJobNotFound(key string) {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return
	}

	err = tc.deleteQueueUnitInstance(namespace, name)
	if err != nil {
		klog.Errorf("Delete queueunit error: %v/%v %v", namespace, name, err.Error())
		return
	}
	klog.Warningf("Delete queueunit %v/%v because can't find related tfjob ", namespace, name)
}

func (tc *ExtensionController) handleErr(err error, key string) {
	if err == nil {
		tc.workqueue.Forget(key)
		return
	}
	// numRequeues defined how many times the item was requeued
	numRequeues := tc.workqueue.NumRequeues(key)
	if numRequeues < MaxQueueRetries {
		tc.workqueue.AddRateLimited(key)
		klog.Infof("We will requeue %v %d times,because:%v, has retried %d times", key, MaxQueueRetries, err, numRequeues+1)
		return
	}

	runtime.HandleError(err)
	klog.Infof("Dropping queueunit %q out of the workqueue: %v", key, err)
	tc.workqueue.Forget(key)
	// If still can't find job after retry, delete queueunit
	tc.deleteQueueUnitWhenJobNotFound(key)
}

func (tc *ExtensionController) enqueueQueueUnit(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	tc.workqueue.AddRateLimited(key)
}

func (tc *ExtensionController) AddQueueUnit(obj interface{}) {
	qu := obj.(*v1alpha1.QueueUnit)
	klog.Infof("Add queueunit:%v/%v", qu.Namespace, qu.Name)
	tc.enqueueQueueUnit(qu)
}

func (tc *ExtensionController) UpdateQueueUnit(oldObj, newObj interface{}) {
	oldQu := oldObj.(*v1alpha1.QueueUnit)
	newQu := newObj.(*v1alpha1.QueueUnit)
	if oldQu.ResourceVersion == newQu.ResourceVersion {
		return
	}
	tc.enqueueQueueUnit(newQu)
}

func (tc *ExtensionController) DeleteQueueUnit(obj interface{}) {
	qu := obj.(*v1alpha1.QueueUnit)
	klog.Infof("QueueUnit deleted:%v/%v", qu.Namespace, qu.Name)
}

func (tc *ExtensionController) AddJob(obj interface{}) {
	job := obj.(*jobv1.Job)
	klog.Infof("Get add job %v/%v event", job.Namespace, job.Name)
	err := tc.createQueueUnitInstance(job)
	if err != nil {
		klog.Errorf("Can't create queueunit for job %v/%v,err is:%v", job.Namespace, job.Name, err)
	}

	if job.Status.Conditions == nil {
		job.Status.Conditions = make([]jobv1.JobCondition, 0)
		job.Status.Conditions = append(job.Status.Conditions, jobv1.JobCondition{
			Type:           Queuing,
			Status:         "True",
			LastTransitionTime: metav1.Now(),
		})
		tc.jobClient.BatchV1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("update job failed Queuing %v/%v %v", job.Namespace, job.Name, err.Error())
		}
		klog.Infof("update job %v/%v status Queuing successfully", job.Namespace, job.Name)
	}
}

func (tc *ExtensionController) createQueueUnitInstance(job *jobv1.Job) error {
	// 1. try to get suspend
	ok := job.Spec.Suspend
	if !*ok {
		klog.Infof("job %v/%v is not scheduled by kube-queue", job.Namespace, job.Name)
		return nil
	}

	// 2. annotation has been found and try to get queueunit from cache
	qu, err := tc.queueInformer.Lister().QueueUnits(job.Namespace).Get(job.Name + QuNameSuffix)
	if err != nil {
		if errors.IsNotFound(err) {
			// 2.1 there is no specified queueunit in k8s
			klog.Infof("Creating queueunit for job %v/%v", job.Namespace, job.Name)
			// 2.2 generate a new queueunit
			quMeta := tc.generateQueueUnitInstance(job)
			// 2.3 create the queueunit
			qu, err = tc.queueClient.SchedulingV1alpha1().QueueUnits(quMeta.Namespace).Create(context.TODO(), quMeta, metav1.CreateOptions{})
			if err != nil {
				return err
			}
			klog.Infof("Created queueunit %v/%v successfully", qu.Namespace, qu.Name)
			return nil
		} else {
			return err
		}
	}

	if qu.Spec.ConsumerRef.Kind == ConsumerRefKind {
		klog.Infof("It already has a queueunit %v/%v for job %v/%v",
			qu.Namespace, qu.Name, job.Namespace, job.Name)
	} else {
		klog.Warningf("There is an exception queueunit:%v/%v for job in k8s, please check it", qu.Namespace, qu.Name)
	}

	return nil
}

func (tc *ExtensionController) generateQueueUnitInstance(job *jobv1.Job) *v1alpha1.QueueUnit {
	// 1. build ObjectReference from corresponding Job CR
	objectReference := tc.generateObjectReference(job)
	// 2. get priorityClassName and priority from one of job roles
	var priorityClassName string
	var priority *int32

	priorityClassName = job.Spec.Template.Spec.PriorityClassName
	if priorityClassName != "" {
		priorityClassInstance, err := tc.k8sClient.SchedulingV1().PriorityClasses().Get(context.TODO(), priorityClassName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				klog.Infof("Can not find priority class name %v in k8s, we will ignore it", priorityClassName)
			} else {
				klog.Errorf("Can not get PriorityClass %v from k8s for job:%v/%v, err:%v", priorityClassName, job.Namespace, job.Name, err)
			}
		} else {
			priority = &priorityClassInstance.Value
		}
	}
	// 3. calculate the total resources of this job instance
	resources := tc.calculateTotalResources(job)
	// 4. build QueueUnit
	return &v1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + QuNameSuffix,
			Namespace: job.Namespace,
		},
		Spec: v1alpha1.QueueUnitSpec{
			ConsumerRef:       objectReference,
			Priority:          priority,
			PriorityClassName: priorityClassName,
			Resource:          resources,
		},
		Status: v1alpha1.QueueUnitStatus{
			Phase:   v1alpha1.Enqueued,
			Message: "the queueunit is enqueued after created",
		},
	}
}

func (tc *ExtensionController) generateObjectReference(job *jobv1.Job) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		APIVersion: ConsumerRefAPIVersion,
		Kind:       ConsumerRefKind,
		Namespace:  job.Namespace,
		Name:       job.Name,
	}
}

func (tc *ExtensionController) calculateTotalResources(job *jobv1.Job) corev1.ResourceList {
	totalResources := corev1.ResourceList{}
	// calculate the total resource request
	count := int(*job.Spec.Completions)
	containers := job.Spec.Template.Spec.Containers
	for _, container := range containers {
		resources := container.Resources.Requests
		for resourceType := range resources {
			quantity := resources[resourceType]
			// scale the quantity by count
			replicaQuantity := resource.Quantity{}
			for i := 1; i <= count; i++ {
				replicaQuantity.Add(quantity)
			}
			// check if the resourceType is in totalResources
			if totalQuantity, ok := totalResources[resourceType]; !ok {
				// not in: set this replicaQuantity
				totalResources[resourceType] = replicaQuantity
			} else {
				// in: append this replicaQuantity and update
				totalQuantity.Add(replicaQuantity)
				totalResources[resourceType] = totalQuantity
			}
		}
	}
	return totalResources
}

func (tc *ExtensionController) UpdateJob(_, newObj interface{}) {
	newTfJob := newObj.(*jobv1.Job)
	conditionsLen := len(newTfJob.Status.Conditions)
	if conditionsLen > 0 {
		lastCondition := newTfJob.Status.Conditions[conditionsLen-1]
		if lastCondition.Type == jobv1.JobFailed || lastCondition.Type == jobv1.JobComplete {
			klog.Infof("job %v/%v finished, current lastCondition.Type: [%v]", newTfJob.Namespace, newTfJob.Name, lastCondition.Type)
			tc.deleteQueueUnitAfterJobTerminated(newTfJob)
		}
	}
}

func (tc *ExtensionController) DeleteJob(obj interface{}) {
	tfJob := obj.(*jobv1.Job)
	klog.Infof("Get delete tfjob %v/%v event", tfJob.Namespace, tfJob.Name)
	tc.deleteQueueUnitAfterJobTerminated(tfJob)
}

func (tc *ExtensionController) deleteQueueUnitAfterJobTerminated(job *jobv1.Job) {
	// Get queueunit from cache
	qu, err := tc.queueInformer.Lister().QueueUnits(job.Namespace).Get(job.Name + QuNameSuffix)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Errorf("failed to get related queueunit by job:%v/%v when delete queueunit, "+
				"maybe qu has been deleted", job.Namespace, job.Name)
			return
		}
	}
	if qu.Spec.ConsumerRef.Name == job.Name && qu.Spec.ConsumerRef.Kind == ConsumerRefKind {
		err = tc.deleteQueueUnitInstance(qu.Namespace, qu.Name)
		if err != nil {
			klog.Errorf("Delete queueunit error: delete qu failed %v/%v %v", qu.Namespace, qu.Name, err)
		}
		klog.Infof("Delete queueunit %s because related job %v/%v terminated", qu.Name, job.Namespace, job.Name)
	}
}

// TODO: How to deal with the failure to delete the queueunit instance
func (tc *ExtensionController) deleteQueueUnitInstance(namespace, name string) error {
	err := tc.queueClient.SchedulingV1alpha1().QueueUnits(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (tc ExtensionController) getJob(namespace, name string) (*jobv1.Job, error) {
	job, err := tc.jobInformer.Lister().Jobs(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Warningf("Can not find related tfjob:%v in namespace:%v", name, namespace)
		}
		return nil, err
	}
	return job, nil
}

func (tc ExtensionController) updateJob(namespace string, job *jobv1.Job) (
	err error, triggerOptimisticLock, triggerOtherError bool) {
	_, err = tc.jobClient.BatchV1().Jobs(namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil && strings.Contains(err.Error(), OptimisticLockErrorMsg) {
		klog.Warningf("Update annotations for job %v/%v failed because of optimisticLock", namespace, job.Name)
		return err, true, false
	}

	if err != nil && !strings.Contains(err.Error(), OptimisticLockErrorMsg) {
		klog.Errorf("Update annotations for job %v/%v failed,%v", namespace, job.Name, err.Error())
		return err, false, true
	}

	return nil, false, false
}

func (tc *ExtensionController) deleteQueueAnnotationInJob(qu *v1alpha1.QueueUnit) error {
	namespace := qu.Spec.ConsumerRef.Namespace
	jobName := qu.Spec.ConsumerRef.Name
	job, err := tc.getJob(namespace, jobName)
	if err != nil {
		klog.Errorf("Get job failed %v/%v %v", namespace, jobName, err.Error())
		return err
	}
	//var annotation = map[string]string{}
	//for k, v := range job.Annotations {
	//	annotation[k] = v
	//}
	suspend := false
	job.Spec.Suspend = &suspend
	//job.SetAnnotations(annotation)
	// TODO change to patch
	err, triggerOptimisticLock, triggerOtherError := tc.updateJob(namespace, job)
	if triggerOtherError == true {
		return err
	}
	// retry when update job fails due to OptimisticLockErrorMsg
	for retry := 0; triggerOptimisticLock == true; retry++ {
		time.Sleep(RetryInterval)
		job, err := tc.getJob(namespace, jobName)
		if err != nil {
			klog.Errorf("Get job %v/%v failed when retrying, %v", namespace, jobName, err.Error())
			return err
		}
		job.Spec.Suspend = &suspend
		//tfJob.SetAnnotations(annotation)
		err, triggerOptimisticLock, triggerOtherError = tc.updateJob(namespace, job)
		if triggerOtherError == true {
			return err
		}
		if retry > MaxUpdateRetries {
			klog.Errorf("After maximum %v retries, it still fails to update job %v/%v : error: %v",
				MaxUpdateRetries, namespace, jobName, err.Error())
			return err
		}
	}

	klog.Infof("Update annotations for job %v/%v successfully", namespace, jobName)
	return nil
}
