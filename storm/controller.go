package storm

import (
	"encoding/json"
	"fmt"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"io/ioutil"
	"net/http"
	"os"
)

const (
	listTopology        string = "/api/v1/topology/summary"
	killTopologyCommand string = "/api/v1/topology/%s/kill/%d"
	stormUrl            string = "STORM_URL"
	kerberosKeytab      string = "KERBEROS_KEYTAB"
	kerberosConfig      string = "KERBEROS_CONF"
	kerberosUser        string = "KERBEROS_USERNAME"
	kerberosDomain      string = "KERBEROS_DOMAIN"
)

type TopologyKillResponseAPI struct {
	TopologyOperation string `json:"topologyOperation"`
	TopologyId        string `json:"topologyId"`
	Status            string `json:"status"`
}

type TopologyAPI struct {
	Topologies []struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	} `json:"topologies"`
}

type StormCluster struct {
	Url    string
	Client interface{}
}

func (s *StormCluster) GetTopologyIdByName(name string) (id *string, e error) {
	r, err := http.NewRequest("GET", s.Url+listTopology, nil)

	var resp *http.Response
	switch c := s.Client.(type) {
	case http.Client:
		resp, _ = c.Do(r)
	case spnego.Client:
		resp, _ = c.Do(r)
	default:
		panic("Cannot find HTTP client for Storm")
	}

	defer resp.Body.Close()
	if err != nil {
		return nil, err
	}

	body, _ := ioutil.ReadAll(resp.Body)

	var m TopologyAPI
	_ = json.Unmarshal(body, &m)
	for _, x := range m.Topologies {
		if x.Name == name {
			return &x.Id, nil
		}
	}
	return nil, nil
}

func (s *StormCluster) KillTopologyByName(name string) TopologyKillResponseAPI {
	id, _ := s.GetTopologyIdByName(name)

	if id != nil {
		api := fmt.Sprintf(killTopologyCommand, *id, 0)
		r, _ := http.NewRequest("POST", s.Url+api, nil)

		var resp *http.Response
		switch c := s.Client.(type) {
		case http.Client:
			resp, _ = c.Do(r)
		case spnego.Client:
			resp, _ = c.Do(r)
		default:
			panic("Cannot find HTTP client for Storm")
		}
		fmt.Println(resp)
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)

		var m TopologyKillResponseAPI
		_ = json.Unmarshal(body, &m)
		return m
	}
	return TopologyKillResponseAPI{}
}

func MakeKerberosStormCluster() StormCluster {
	conf := os.Getenv(kerberosConfig)
	domain := os.Getenv(kerberosDomain)
	ktPath := os.Getenv(kerberosKeytab)
	user := os.Getenv(kerberosUser)
	url := os.Getenv(stormUrl)

	// Get ENV kerberos.keytab & kerberos.config & kerberos.domain & kerberos.user
	cfg, _ := config.Load(conf)
	kt, _ := keytab.Load(ktPath)
	cl := client.NewWithKeytab(user, domain, kt, cfg)
	spnegoCl := spnego.NewClient(cl, nil, "")
	return StormCluster{
		Url:    url,
		Client: spnegoCl,
	}
}

func MakeHttpStormCluster() StormCluster {
	url := os.Getenv(stormUrl)

	var httpCl http.Client = *http.DefaultClient
	return StormCluster{
		Url:    url,
		Client: httpCl,
	}
}

func MakeStormClusterFromEnvironment() StormCluster {
	if checkEnv(kerberosUser) && checkEnv(kerberosKeytab) && checkEnv(kerberosDomain) && checkEnv(kerberosConfig) {
		return MakeKerberosStormCluster()
	} else {
		return MakeHttpStormCluster()
	}
}

func checkEnv(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}
