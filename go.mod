module github.com/kube-queue/job-extension

go 1.15

require (
	github.com/kube-queue/api v0.0.0-20210623033849-bffe1acb5aa9
	github.com/sirupsen/logrus v1.6.0 // indirect
	k8s.io/api v0.18.5
	k8s.io/apimachinery v0.18.5
	k8s.io/client-go v0.18.5
	k8s.io/code-generator v0.18.5
	k8s.io/klog/v2 v2.30.0
	k8s.io/kube-openapi v0.0.0-20211115234752-e816edb12b65
	k8s.io/sample-controller v0.0.0
)

replace (
	k8s.io/api => ./staging/src/k8s.io/api
	k8s.io/apimachinery => ./staging/src/k8s.io/apimachinery
	k8s.io/client-go => ./staging/src/k8s.io/client-go
	k8s.io/code-generator => ./staging/src/k8s.io/code-generator
	k8s.io/sample-controller => ./staging/src/k8s.io/sample-controller
)
