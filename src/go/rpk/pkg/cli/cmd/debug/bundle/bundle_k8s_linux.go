// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package bundle

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/hashicorp/go-multierror"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func executeK8SBundle(ctx context.Context, bp bundleParams) error {
	fmt.Println("Creating bundle file...")
	mode := os.FileMode(0o755)
	f, err := bp.fs.OpenFile(
		bp.path,
		os.O_CREATE|os.O_WRONLY,
		mode,
	)
	if err != nil {
		return fmt.Errorf("unable to create bundle file: %v", err)
	}
	defer f.Close()

	var grp multierror.Group

	w := zip.NewWriter(f)
	defer w.Close()

	ps := &stepParams{
		fs:      bp.fs,
		w:       w,
		timeout: bp.timeout,
	}
	var errs *multierror.Error

	steps := []step{
		saveKafkaMetadata(ctx, ps, bp.cl),
		saveDataDirStructure(ps, bp.cfg),
		saveConfig(ps, bp.cfg),
		saveCPUInfo(ps),
		saveInterrupts(ps),
		saveResourceUsageData(ps, bp.cfg),
		saveNTPDrift(ps),
		saveDiskUsage(ctx, ps, bp.cfg),
		saveControllerLogDir(ps, bp.cfg, bp.controllerLogLimitBytes),
	}

	adminAddresses, err := adminAddressesFromK8S(ctx)
	if err != nil {
		errs = multierror.Append(errs, fmt.Errorf("skipping admin API calls, unable to get admin API addresses: %v", err))
	} else {
		steps = append(steps, []step{
			saveClusterAdminAPICalls(ctx, ps, bp.fs, bp.cfg, adminAddresses),
			saveSingleAdminAPICalls(ctx, ps, bp.fs, bp.cfg, adminAddresses),
		}...)
	}
	for _, s := range steps {
		grp.Go(s)
	}

	stepErrs := grp.Wait()
	if stepErrs != nil {
		errs = multierror.Append(errs, stepErrs.ErrorOrNil())
		err := writeFileToZip(ps, "errors.txt", []byte(errs.Error()))
		if err != nil {
			errs = multierror.Append(errs, err)
		}
		fmt.Println(errs.Error())
	}

	fmt.Printf("Debug bundle saved to %q\n", f.Name())
	return nil
}

// adminAddressesFromK8S returns the admin API host:port list by querying the
// K8S Api.
func adminAddressesFromK8S(ctx context.Context) ([]string, error) {
	// This is intended to run only in a k8s cluster:
	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to get kubernetes cluster configuration: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create kubernetes client: %v", err)
	}

	// Get pods in the 'redpanda' namespace.
	pods, err := clientset.CoreV1().Pods("redpanda").List(ctx, v1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to get kubernetes pods: %v", err)
	}

	// Get the admin addresses from ContainerPort.
	var adminAddresses []string
	for _, p := range pods.Items {
		for _, port := range p.Spec.Containers[0].Ports {
			if port.Name == "admin" {
				a := fmt.Sprintf("%v:%v", p.Status.PodIP, port.ContainerPort)
				adminAddresses = append(adminAddresses, a)
			}
		}
	}

	return adminAddresses, nil
}

// saveClusterAdminAPICalls save the following admin API request to the zip:
//   - Cluster Health: /v1/cluster/health_overview
//   - Brokers: /v1/brokers
//   - License Info: /v1/features/license
//   - Cluster Config: /v1/cluster_config
//   - Reconfigurations: /v1/partitions/reconfigurations
func saveClusterAdminAPICalls(ctx context.Context, ps *stepParams, fs afero.Fs, cfg *config.Config, adminAddresses []string) step {
	return func() error {
		allCfg := &config.Config{
			Rpk: config.RpkConfig{
				AdminAPI: config.RpkAdminAPI{
					Addresses: adminAddresses,
					TLS:       cfg.Rpk.AdminAPI.TLS,
				},
			},
		}
		cl, err := admin.NewClient(fs, allCfg)
		if err != nil {
			return fmt.Errorf("unable to initialize admin client: %v", err)
		}

		var grp multierror.Group
		for _, f := range []func() error{
			func() error { return requestAndSave(ctx, ps, "admin/brokers.json", cl.Brokers) },
			func() error { return requestAndSave(ctx, ps, "admin/health_overview.json", cl.GetHealthOverview) },
			func() error { return requestAndSave(ctx, ps, "admin/license.json", cl.GetLicenseInfo) },
			func() error { return requestAndSave(ctx, ps, "admin/reconfigurations.json", cl.Reconfigurations) },
			func() error {
				// Need to wrap this function because cl.Config receives an additional 'includeDefaults' param.
				f := func(ctx context.Context) (admin.Config, error) {
					return cl.Config(ctx, true)
				}
				return requestAndSave(ctx, ps, "admin/cluster_config.json", f)
			},
		} {
			grp.Go(f)
		}
		errs := grp.Wait()
		return errs.ErrorOrNil()
	}
}

// saveSingleAdminAPICalls save the following per-node admin API request to the
// zip:
//   - Node Config: /v1/node_config
//   - Prometheus Metrics: /metrics and /public_metrics
//   - Cluster View: v1/cluster_view
func saveSingleAdminAPICalls(ctx context.Context, ps *stepParams, fs afero.Fs, cfg *config.Config, adminAddresses []string) step {
	return func() error {
		var rerrs *multierror.Error
		var funcs []func() error
		for _, a := range adminAddresses {
			a := a
			c := &config.Config{
				Rpk: config.RpkConfig{
					AdminAPI: config.RpkAdminAPI{
						Addresses: []string{a},
						TLS:       cfg.Rpk.AdminAPI.TLS,
					},
				},
			}
			cl, err := admin.NewClient(fs, c)
			if err != nil {
				rerrs = multierror.Append(rerrs, fmt.Errorf("unable to initialize admin client for %q: %v", a, err))
				continue
			}

			r := []func() error{
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/node_config_%v.json", a), cl.RawNodeConfig)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/cluster_view_%v.json", a), cl.ClusterView)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("metrics/metric_%v.txt", a), cl.PrometheusMetrics)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("metrics/public_metrics_%v.txt", a), cl.PublicMetrics)
				},
			}
			funcs = append(funcs, r...)
		}

		var grp multierror.Group
		for _, f := range funcs {
			grp.Go(f)
		}
		errs := grp.Wait()
		if errs != nil {
			rerrs = multierror.Append(rerrs, errs)
		}
		return rerrs.ErrorOrNil()
	}
}

// requestAndSave receives a callback function f to be executed and marshals the
// response into a json object that is stored in the zip writer.
func requestAndSave[T1 any](ctx context.Context, ps *stepParams, filename string, f func(ctx context.Context) (T1, error)) error {
	object, err := f(ctx)
	if err != nil {
		return fmt.Errorf("unable to issue request for %q: %v", filename, err)
	}

	switch t := any(object).(type) {
	case []byte:
		err = writeFileToZip(ps, filename, t)
		if err != nil {
			return fmt.Errorf("unable to save output for %q: %v", filename, err)
		}
	default:
		b, err := json.Marshal(object)
		if err != nil {
			return fmt.Errorf("unable to marshal broker response for %q: %v", filename, err)
		}
		err = writeFileToZip(ps, filename, b)
		if err != nil {
			return fmt.Errorf("unable to save output for %q: %v", filename, err)
		}
	}
	return nil
}
