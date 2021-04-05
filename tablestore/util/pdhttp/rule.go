package pdhttp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"path"

	"github.com/pingcap/pd/v4/server/schedule/placement"
)

var (
	configPrefix          = "pd/api/v1/config"
	schedulePrefix        = "pd/api/v1/config/schedule"
	replicatePrefix       = "pd/api/v1/config/replicate"
	labelPropertyPrefix   = "pd/api/v1/config/label-property"
	clusterVersionPrefix  = "pd/api/v1/config/cluster-version"
	rulesPrefix           = "pd/api/v1/config/rules"
	rulesBatchPrefix      = "pd/api/v1/config/rules/batch"
	rulePrefix            = "pd/api/v1/config/rule"
	ruleGroupPrefix       = "pd/api/v1/config/rule_group"
	ruleGroupsPrefix      = "pd/api/v1/config/rule_groups"
	replicationModePrefix = "pd/api/v1/config/replication-mode"
	ruleBundlePrefix      = "pd/api/v1/config/placement-rule"
)

func PutPlacementRules(rules []*placement.Rule) error {
	var err error
	for _, r := range rules {
		if r.Count > 0 {
			b, _ := json.Marshal(r)
			_, err = doRequest(rulePrefix, http.MethodPost, WithBody("application/json", bytes.NewBuffer(b)))
			if err != nil {
				return err
			}
		}
	}
	for _, r := range rules {
		if r.Count == 0 {
			_, err = doRequest(path.Join(rulePrefix, r.GroupID, r.ID), http.MethodDelete)
			if err != nil {
				return err
			}
		}
	}
	return err
}
