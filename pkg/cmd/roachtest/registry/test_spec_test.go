// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package registry

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/stretchr/testify/require"
)

func TestCloudSet(t *testing.T) {
	expect := func(c CloudSet, exp string) {
		require.Equal(t, exp, c.String())
	}
	expect(AllClouds, "local,gce,aws,azure,ibm")
	expect(AllExceptAWS, "local,gce,azure,ibm")
	expect(AllExceptLocal, "gce,aws,azure,ibm")
	expect(AllExceptLocal.NoAWS(), "gce,azure,ibm")
	expect(AllClouds.NoAWS().NoAzure(), "local,gce,ibm")
	expect(AllClouds.NoIBM(), "local,gce,aws,azure")

	require.True(t, AllExceptAWS.Contains(spec.GCE))
	require.True(t, AllExceptAWS.Contains(spec.Local))
	require.False(t, AllExceptAWS.Contains(spec.AWS))
}

func TestSuiteSet(t *testing.T) {
	expect := func(c SuiteSet, exp string) {
		require.Equal(t, exp, c.String())
	}
	s := Suites(Nightly, Weekly)
	expect(s, "nightly,weekly")
	require.True(t, s.Contains(Nightly))
	require.True(t, s.Contains(Weekly))
	require.False(t, s.Contains(ReleaseQualification))
	expect(ManualOnly, "<none>")
}
