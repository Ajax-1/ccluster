/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/

package cp

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/runtime/schema"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cclusternvidiacomv1alpha1 "github.com/NVIDIA/ccluster/api/ccluster/v1alpha1"
	"github.com/NVIDIA/ccluster/catalogs"
	argocdv1alpha1 "github.com/NVIDIA/ccluster/internal/api/argocd/v1alpha1"
	"github.com/NVIDIA/ccluster/internal/argocd/apiclient"
)

// An old object is one a component's chart no longer renders but Argo CD still keeps:
// status.resources marks it requiresPruning. See DESIGN.md, "Objects deleted after rollout".

// deletableKinds are the kinds of old object the operator deletes, and the only kinds the
// cp chart's policy lets its Argo CD account delete. An old object of another kind stays:
// a chart that marks one Prune=false means to keep it.
var deletableKinds = []schema.GroupKind{
	{Group: "external-secrets.io", Kind: "ExternalSecret"},
	{Group: "", Kind: "ConfigMap"},
	{Group: "resource.k8s.io", Kind: "ResourceClaimTemplate"},
}

// objectKey identifies an object across status.resources and the resource tree.
type objectKey struct {
	Group, Kind, Namespace, Name string
}

func statusKey(r argocdv1alpha1.ResourceStatus) objectKey {
	return objectKey{Group: r.Group, Kind: r.Kind, Namespace: r.Namespace, Name: r.Name}
}

func treeKey(r catalogs.ResourceRef) objectKey {
	return objectKey{Group: r.Group, Kind: r.Kind, Namespace: r.Namespace, Name: r.Name}
}

// deletableOldObjects returns application's old objects of deletableKinds.
func deletableOldObjects(application *argocdv1alpha1.Application) []argocdv1alpha1.ResourceStatus {
	var deletable []argocdv1alpha1.ResourceStatus
	for _, r := range application.OldObjects() {
		if slices.Contains(deletableKinds, schema.GroupKind{Group: r.Group, Kind: r.Kind}) {
			deletable = append(deletable, r)
		}
	}
	return deletable
}

// withoutOldObjects leaves application's old objects, and what they own, out of observed.
// Argo CD's aggregate health still counts them, so where it is worse than every rendered
// object left, it becomes the worst of those. See DESIGN.md, "Objects deleted after rollout".
func withoutOldObjects(
	observed catalogs.Observation, application *argocdv1alpha1.Application,
) catalogs.Observation {
	old := map[objectKey]bool{}
	for _, r := range application.OldObjects() {
		old[statusKey(r)] = true
	}
	if len(old) == 0 {
		return observed
	}

	leftOut := map[catalogs.ResourceRef]bool{}
	var leaveOut func(ref catalogs.ResourceRef)
	leaveOut = func(ref catalogs.ResourceRef) {
		if leftOut[ref] {
			return
		}
		leftOut[ref] = true
		for _, child := range observed.Resources.ChildrenOf(ref) {
			leaveOut(child.ResourceRef)
		}
	}
	for _, r := range observed.Resources {
		if r.Rendered() && old[treeKey(r.ResourceRef)] {
			leaveOut(r.ResourceRef)
		}
	}

	var remaining catalogs.ObservedResources
	worst := cclusternvidiacomv1alpha1.ComponentHealthHealthy
	for _, r := range observed.Resources {
		if leftOut[r.ResourceRef] {
			continue
		}
		remaining = append(remaining, r)
		if health := catalogs.FromArgoCD(r.Health); r.Rendered() && r.Health != "" && catalogs.Worse(health, worst) {
			worst = health
		}
	}
	observed.Resources = remaining
	if observed.ApplicationHealth != "" && catalogs.Worse(catalogs.FromArgoCD(observed.ApplicationHealth), worst) {
		observed.ApplicationHealth = string(worst)
	}
	return observed
}

// deleteOldObjects deletes the deletable old objects of finished, the Applications of the
// components that have finished rolling out, skipping those already being deleted. All its
// requests share one readTimeout. See DESIGN.md, "Objects deleted after rollout".
func (r *CCLComponentSetReconciler) deleteOldObjects(
	ctx context.Context, finished []*argocdv1alpha1.Application,
) error {
	if r.ArgoCD == nil {
		return nil
	}
	log := logf.FromContext(ctx)
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	var errs []error
	for _, application := range finished {
		old := deletableOldObjects(application)
		if len(old) == 0 {
			continue
		}
		app := applicationRef(application)
		tree, err := r.ArgoCD.ResourceTree(ctx, app)
		if err != nil {
			errs = append(errs, fmt.Errorf("reading the resource tree of Application %s: %w", application.Name, err))
			continue
		}
		terminating := map[objectKey]bool{}
		for _, node := range tree.Nodes {
			if node.Terminating() {
				terminating[treeKey(catalogRef(node.ResourceRef))] = true
			}
		}

		for _, obj := range old {
			if terminating[statusKey(obj)] {
				continue
			}
			err := r.ArgoCD.DeleteResource(ctx, app, apiclient.ResourceRef{
				Group: obj.Group, Version: obj.Version, Kind: obj.Kind, Namespace: obj.Namespace, Name: obj.Name,
			})
			if errors.Is(err, apiclient.ErrResourceGone) {
				continue
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("deleting %s %s/%s of Application %s: %w",
					obj.Kind, obj.Namespace, obj.Name, application.Name, err))
				continue
			}
			log.Info("Deleted old object of Argo CD Application", "application", application.Name,
				"namespace", application.Namespace, "kind", obj.Kind, "object", obj.Namespace+"/"+obj.Name)
		}
	}
	return errors.Join(errs...)
}
