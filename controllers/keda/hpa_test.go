/*
Copyright 2021 The KEDA Authors

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

package keda

import (
	"context"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	v2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/mock/mock_client"
	mock_scalers "github.com/kedacore/keda/v2/pkg/mock/mock_scaler"
	"github.com/kedacore/keda/v2/pkg/mock/mock_scaling"
	"github.com/kedacore/keda/v2/pkg/scalers"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	"github.com/kedacore/keda/v2/pkg/scaling/cache"
)

var _ = Describe("hpa", func() {
	var (
		reconciler   ScaledObjectReconciler
		scaleHandler *mock_scaling.MockScaleHandler
		client       *mock_client.MockClient
		statusWriter *mock_client.MockStatusWriter
		scaler       *mock_scalers.MockScaler
		logger       logr.Logger
		ctrl         *gomock.Controller
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		client = mock_client.NewMockClient(ctrl)
		scaleHandler = mock_scaling.NewMockScaleHandler(ctrl)
		scaler = mock_scalers.NewMockScaler(ctrl)
		statusWriter = mock_client.NewMockStatusWriter(ctrl)
		logger = logr.Discard()
		reconciler = ScaledObjectReconciler{
			Client:       client,
			ScaleHandler: scaleHandler,
		}
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	It("should remove deleted metric from health status", func() {
		numberOfFailures := int32(87)
		health := make(map[string]v1alpha1.HealthStatus)
		health["another metric name"] = v1alpha1.HealthStatus{
			NumberOfFailures: &numberOfFailures,
			Status:           v1alpha1.HealthStatusFailing,
		}

		scaledObject := setupTest(health, scaler, scaleHandler)

		var capturedScaledObject v1alpha1.ScaledObject
		client.EXPECT().Status().Return(statusWriter)
		statusWriter.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(arg interface{}, scaledObject *v1alpha1.ScaledObject, anotherArg interface{}, opts ...interface{}) {
			capturedScaledObject = *scaledObject
		})

		_, err := reconciler.getScaledObjectMetricSpecs(context.Background(), logger, scaledObject)

		Expect(err).ToNot(HaveOccurred())
		Expect(capturedScaledObject.Status.Health).To(BeEmpty())
	})

	It("should not remove existing metric from health status", func() {
		numberOfFailures := int32(87)
		health := make(map[string]v1alpha1.HealthStatus)
		health["another metric name"] = v1alpha1.HealthStatus{
			NumberOfFailures: &numberOfFailures,
			Status:           v1alpha1.HealthStatusFailing,
		}

		health["some metric name"] = v1alpha1.HealthStatus{
			NumberOfFailures: &numberOfFailures,
			Status:           v1alpha1.HealthStatusFailing,
		}

		scaledObject := setupTest(health, scaler, scaleHandler)

		var capturedScaledObject v1alpha1.ScaledObject
		client.EXPECT().Status().Return(statusWriter)
		statusWriter.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(arg interface{}, scaledObject *v1alpha1.ScaledObject, anotherArg interface{}, opts ...interface{}) {
			capturedScaledObject = *scaledObject
		})

		_, err := reconciler.getScaledObjectMetricSpecs(context.Background(), logger, scaledObject)

		expectedHealth := make(map[string]v1alpha1.HealthStatus)
		expectedHealth["some metric name"] = v1alpha1.HealthStatus{
			NumberOfFailures: &numberOfFailures,
			Status:           v1alpha1.HealthStatusFailing,
		}

		Expect(err).ToNot(HaveOccurred())
		Expect(capturedScaledObject.Status.Health).To(HaveLen(1))
		Expect(capturedScaledObject.Status.Health).To(Equal(expectedHealth))
	})

	It("should add both name-hash and name labels when name is <= 63 characters", func() {
		health := make(map[string]v1alpha1.HealthStatus)
		scaledObject := setupTest(health, scaler, scaleHandler)

		client.EXPECT().Status().Return(statusWriter)
		statusWriter.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any())

		metricSpecs, err := reconciler.getScaledObjectMetricSpecs(context.Background(), logger, scaledObject)

		Expect(err).ToNot(HaveOccurred())
		Expect(metricSpecs).To(HaveLen(1))
		Expect(metricSpecs[0].External).ToNot(BeNil())
		Expect(metricSpecs[0].External.Metric.Selector).ToNot(BeNil())
		// Verify both labels are present
		Expect(metricSpecs[0].External.Metric.Selector.MatchLabels).To(HaveKey(v1alpha1.ScaledObjectOwnerHashAnnotation))
		Expect(metricSpecs[0].External.Metric.Selector.MatchLabels).To(HaveKey(v1alpha1.ScaledObjectOwnerAnnotation))
		// Verify name label has the correct value
		Expect(metricSpecs[0].External.Metric.Selector.MatchLabels[v1alpha1.ScaledObjectOwnerAnnotation]).To(Equal(scaledObject.Name))
		// Verify hash label is 32 characters
		hashLabel := metricSpecs[0].External.Metric.Selector.MatchLabels[v1alpha1.ScaledObjectOwnerHashAnnotation]
		Expect(len(hashLabel)).To(Equal(v1alpha1.ScaledObjectNameHashLength))
	})

	It("should add only name-hash label when name is > 63 characters", func() {
		health := make(map[string]v1alpha1.HealthStatus)
		scaledObject := setupTestWithLongName(health, scaler, scaleHandler)

		client.EXPECT().Status().Return(statusWriter)
		statusWriter.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any())

		metricSpecs, err := reconciler.getScaledObjectMetricSpecs(context.Background(), logger, scaledObject)

		Expect(err).ToNot(HaveOccurred())
		Expect(metricSpecs).To(HaveLen(1))
		Expect(metricSpecs[0].External).ToNot(BeNil())
		Expect(metricSpecs[0].External.Metric.Selector).ToNot(BeNil())
		// Verify only hash label is present (name would exceed 63 chars)
		Expect(metricSpecs[0].External.Metric.Selector.MatchLabels).To(HaveKey(v1alpha1.ScaledObjectOwnerHashAnnotation))
		Expect(metricSpecs[0].External.Metric.Selector.MatchLabels).ToNot(HaveKey(v1alpha1.ScaledObjectOwnerAnnotation))
		// Verify hash label is 32 characters (always valid as label value)
		hashLabel := metricSpecs[0].External.Metric.Selector.MatchLabels[v1alpha1.ScaledObjectOwnerHashAnnotation]
		Expect(len(hashLabel)).To(Equal(v1alpha1.ScaledObjectNameHashLength))
		Expect(len(hashLabel)).To(BeNumerically("<=", 63))
	})

	It("should generate deterministic hash for same namespace/name", func() {
		namespace := "test-namespace"
		name := "test-scaledobject"
		hash1 := v1alpha1.GenerateScaledObjectNameHash(namespace, name)
		hash2 := v1alpha1.GenerateScaledObjectNameHash(namespace, name)
		Expect(hash1).To(Equal(hash2))
		Expect(len(hash1)).To(Equal(v1alpha1.ScaledObjectNameHashLength))
	})

	It("should generate different hashes for different names", func() {
		namespace := "test-namespace"
		hash1 := v1alpha1.GenerateScaledObjectNameHash(namespace, "name1")
		hash2 := v1alpha1.GenerateScaledObjectNameHash(namespace, "name2")
		Expect(hash1).ToNot(Equal(hash2))
	})

})

func setupTest(health map[string]v1alpha1.HealthStatus, scaler *mock_scalers.MockScaler, scaleHandler *mock_scaling.MockScaleHandler) *v1alpha1.ScaledObject {
	scaledObject := &v1alpha1.ScaledObject{
		ObjectMeta: v1.ObjectMeta{
			Name:      "some scaled object name",
			Namespace: "default",
		},
		Status: v1alpha1.ScaledObjectStatus{
			Health: health,
		},
	}

	scalersCache := cache.ScalersCache{
		Scalers: []cache.ScalerBuilder{{
			Scaler: scaler,
			Factory: func() (scalers.Scaler, *scalersconfig.ScalerConfig, error) {
				return scaler, &scalersconfig.ScalerConfig{}, nil
			},
		}},
		Recorder: nil,
	}
	metricSpec := v2.MetricSpec{
		External: &v2.ExternalMetricSource{
			Metric: v2.MetricIdentifier{
				Name: "some metric name",
			},
		},
	}
	metricSpecs := []v2.MetricSpec{metricSpec}
	ctx := context.Background()
	scaler.EXPECT().GetMetricSpecForScaling(ctx).Return(metricSpecs)
	scaleHandler.EXPECT().GetScalersCache(context.Background(), gomock.Eq(scaledObject)).Return(&scalersCache, nil)

	return scaledObject
}

func setupTestWithLongName(health map[string]v1alpha1.HealthStatus, scaler *mock_scalers.MockScaler, scaleHandler *mock_scaling.MockScaleHandler) *v1alpha1.ScaledObject {
	// Create a name longer than 63 characters to test the fix for issue #6998
	longName := "this-is-a-very-long-scaled-object-name-that-exceeds-63-characters-limit-test"
	scaledObject := &v1alpha1.ScaledObject{
		ObjectMeta: v1.ObjectMeta{
			Name:      longName,
			Namespace: "default",
		},
		Status: v1alpha1.ScaledObjectStatus{
			Health: health,
		},
	}

	scalersCache := cache.ScalersCache{
		Scalers: []cache.ScalerBuilder{{
			Scaler: scaler,
			Factory: func() (scalers.Scaler, *scalersconfig.ScalerConfig, error) {
				return scaler, &scalersconfig.ScalerConfig{}, nil
			},
		}},
		Recorder: nil,
	}
	metricSpec := v2.MetricSpec{
		External: &v2.ExternalMetricSource{
			Metric: v2.MetricIdentifier{
				Name: "some metric name",
			},
		},
	}
	metricSpecs := []v2.MetricSpec{metricSpec}
	ctx := context.Background()
	scaler.EXPECT().GetMetricSpecForScaling(ctx).Return(metricSpecs)
	scaleHandler.EXPECT().GetScalersCache(context.Background(), gomock.Eq(scaledObject)).Return(&scalersCache, nil)

	return scaledObject
}
