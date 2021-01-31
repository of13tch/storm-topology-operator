package storm

import (
	"encoding/json"
	"fmt"
	"github.com/go-logr/logr"
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
	kerberosPrincipal   string = "KERBEROS_PRINCIPAL"
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

type StormClient struct {
	Log             logr.Logger
	Url             string
	KerberosClient  *client.Client
	HttpClient      *spnego.Client
	kerberosEnabled bool
}

func (s *StormClient) Do(r *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	if s.kerberosEnabled {
		s.KerberosClient.Login()
	}

	resp, err = s.HttpClient.Do(r)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *StormClient) GetTopologyIdByName(name string) (id *string, e error) {
	r, err := http.NewRequest("GET", s.Url+listTopology, nil)
	resp, err := s.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var m TopologyAPI
	err = json.Unmarshal(body, &m)
	if err != nil {
		return nil, err
	}

	for _, x := range m.Topologies {
		if x.Name == name {
			return &x.Id, nil
		}
	}
	return nil, nil
}

func (s *StormClient) KillTopologyByName(name string) (TopologyKillResponseAPI, error) {
	id, _ := s.GetTopologyIdByName(name)

	if id != nil {
		api := fmt.Sprintf(killTopologyCommand, *id, 0)
		r, _ := http.NewRequest("POST", s.Url+api, nil)
		resp, err := s.Do(r)
		if err != nil {
			return TopologyKillResponseAPI{}, err
		}
		defer resp.Body.Close()

		body, _ := ioutil.ReadAll(resp.Body)
		s.Log.Info("Topology kill request made: " + name + ". Status: " + resp.Status + ". Response: " + string(body))
		var m TopologyKillResponseAPI
		_ = json.Unmarshal(body, &m)
		return m, nil
	}
	return TopologyKillResponseAPI{}, nil
}

func MakeStormCluster() StormClient {
	url := os.Getenv(stormUrl)

	if kerberosEnabled() {
		conf, domain, ktPath, user := os.Getenv(kerberosConfig), os.Getenv(kerberosDomain), os.Getenv(kerberosKeytab),
			os.Getenv(kerberosPrincipal)

		// Get ENV kerberos.keytab & kerberos.config & kerberos.domain & kerberos.user
		cfg, _ := config.Load(conf)
		kt, _ := keytab.Load(ktPath)
		cl := client.NewWithKeytab(user, domain, kt, cfg, client.DisablePAFXFAST(true))

		err := cl.Login()
		if err != nil {
			panic(err)
		}

		spnegoCl := spnego.NewClient(cl, nil, "")
		return StormClient{
			kerberosEnabled: true,
			Url:             url,
			HttpClient:      spnegoCl,
			KerberosClient:  cl,
		}
	} else {
		httpclient := spnego.NewClient(nil, nil, "")
		return StormClient{
			kerberosEnabled: false,
			Url:             url,
			HttpClient:      httpclient,
		}
	}
}

func checkEnv(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}

func kerberosEnabled() bool {
	return checkEnv(kerberosPrincipal) && checkEnv(kerberosKeytab) &&
		checkEnv(kerberosDomain) && checkEnv(kerberosConfig)
}
