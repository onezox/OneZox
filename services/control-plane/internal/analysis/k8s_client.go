package analysis

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// analysisRunGVR — argoproj.io/v1alpha1 AnalysisRun, the CRD Argo
// Rollouts installed (Phase-05 Step A) already registers.
var analysisRunGVR = schema.GroupVersionResource{
	Group: "argoproj.io", Version: "v1alpha1", Resource: "analysisruns",
}

const (
	labelRolloutID = "onezox.io/rollout-id"
	labelStage     = "onezox.io/stage"
)

// K8sClient is the real, dynamic-client-backed Client implementation —
// unstructured.Unstructured, not a typed Argo Rollouts clientset: this
// package needs exactly one CRD's worth of Create/List, not the full
// generated client for every Argo Rollouts type, the same "narrow
// dependency, not the whole SDK" reasoning provider-gateway's own
// credentials.TokenFetcher and admin-api's own controlPublisher already
// established for their own external boundaries.
type K8sClient struct {
	dyn       dynamic.Interface
	namespace string
	promAddr  string
}

func NewK8sClient(dyn dynamic.Interface, namespace, promAddr string) *K8sClient {
	return &K8sClient{dyn: dyn, namespace: namespace, promAddr: promAddr}
}

func (c *K8sClient) FindForStage(ctx context.Context, rolloutID, stage string) (*Run, error) {
	list, err := c.dyn.Resource(analysisRunGVR).Namespace(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", labelRolloutID, rolloutID, labelStage, stage),
	})
	if err != nil {
		return nil, fmt.Errorf("listing AnalysisRuns: %w", err)
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	// By this package's own labeling convention exactly one should ever
	// exist; if a bug or a manual kubectl apply ever produced more than
	// one, take the first rather than erroring — a defensive choice, not
	// a documented multi-AnalysisRun-per-stage feature.
	item := list.Items[0]
	phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
	return &Run{Name: item.GetName(), Phase: Phase(phase)}, nil
}

func (c *K8sClient) CreateForStage(ctx context.Context, rolloutID, modelRef, stage string, canaryPercent int32) error {
	name := fmt.Sprintf("onezox-canary-%s-%s", rolloutID, uuid.NewString()[:8])
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "argoproj.io/v1alpha1",
			"kind":       "AnalysisRun",
			"metadata": map[string]any{
				"name":      name,
				"namespace": c.namespace,
				"labels": map[string]any{
					labelRolloutID: rolloutID,
					labelStage:     stage,
				},
			},
			"spec": map[string]any{
				"metrics": []any{
					map[string]any{
						"name":  "canary-error-rate",
						"count": int64(1),
						"provider": map[string]any{
							"prometheus": map[string]any{
								"address": c.promAddr,
								"query":   queryFor(modelRef),
							},
						},
						// Healthy: canary error rate under 50% — a
						// deliberately loose threshold for a local-dev
						// proof, not a tuned production SLO (Phase-05.txt
						// itself doesn't specify one). "result[0] < 0.5"
						// reads the Prometheus vector's own first sample.
						"successCondition": "result[0] < 0.5",
						"failureCondition": "result[0] >= 0.5",
						// consecutiveErrorLimit, not left at Argo Rollouts'
						// own unbounded-retry default: a query evaluation
						// ERROR (as opposed to a clean pass/fail) — e.g. no
						// data yet, confirmed live against a real cluster
						// before Step N's own metric existed — would
						// otherwise retry forever, leaving a real rollout
						// stuck Running indefinitely instead of resolving
						// one way or the other. 3 consecutive errors
						// terminalizes to "Error," which this package's own
						// Phase handling already treats as a rollback
						// trigger (reconciler.go) — a query that can't get
						// real data fails SAFE (reverts to stable) rather
						// than hanging, the same "don't silently wait
						// forever" discipline this project applies
						// everywhere else.
						"consecutiveErrorLimit": int64(3),
					},
				},
			},
		},
	}
	_, err := c.dyn.Resource(analysisRunGVR).Namespace(c.namespace).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating AnalysisRun: %w", err)
	}
	return nil
}
