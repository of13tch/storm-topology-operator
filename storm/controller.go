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

type StormCluster struct {
	Log            logr.Logger
	Url            string
	KerberosClient *client.Client
	HttpClient     interface{}
}

func (s *StormCluster) Do(r *http.Request) (*http.Response, error) {
	s.Log.Info("Making request to Storm" + r.Method + " " + r.RemoteAddr)
	var resp *http.Response
	var err error
	switch c := s.HttpClient.(type) {
	case http.Client:
		s.Log.Info("Invoking the http.Client")
		resp, err = c.Do(r)
		if err != nil {
			return nil, err
		}
	case *spnego.Client:
		s.Log.Info("Invoking the spenego.Client")
		s.KerberosClient.Login()
		resp, err = c.Do(r)
		if err != nil {
			return nil, err
		}
	default:
		panic("No HTTP client implemented")
	}
	return resp, nil
}

func (s *StormCluster) GetTopologyIdByName(name string) (id *string, e error) {
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

func (s *StormCluster) KillTopologyByName(name string) (TopologyKillResponseAPI, error) {
	id, _ := s.GetTopologyIdByName(name)

	if id != nil {
		api := fmt.Sprintf(killTopologyCommand, *id, 0)
		r, _ := http.NewRequest("POST", s.Url+api, nil)
		resp, err := s.Do(r)
		if err != nil {
			return TopologyKillResponseAPI{}, err
		}
		defer resp.Body.Close()

		fmt.Println(resp)
		body, _ := ioutil.ReadAll(resp.Body)
		var m TopologyKillResponseAPI
		_ = json.Unmarshal(body, &m)
		return m, nil
	}
	return TopologyKillResponseAPI{}, nil
}

func MakeKerberosStormCluster() StormCluster {
	conf, domain, ktPath, user, url := os.Getenv(kerberosConfig), os.Getenv(kerberosDomain), os.Getenv(kerberosKeytab),
		os.Getenv(kerberosPrincipal), os.Getenv(stormUrl)

	// Get ENV kerberos.keytab & kerberos.config & kerberos.domain & kerberos.user
	cfg, _ := config.Load(conf)
	kt, _ := keytab.Load(ktPath)
	cl := client.NewWithKeytab(user, domain, kt, cfg, client.DisablePAFXFAST(true))

	err := cl.Login()
	if err != nil {
		panic(err)
	}
	spnegoCl := spnego.NewClient(cl, nil, "")
	return StormCluster{
		Url:            url,
		HttpClient:     spnegoCl,
		KerberosClient: cl,
	}
}

func MakeHttpStormCluster() StormCluster {
	url := os.Getenv(stormUrl)

	httpCl := *http.DefaultClient
	return StormCluster{
		Url:            url,
		HttpClient:     httpCl,
		KerberosClient: nil,
	}
}

func MakeStormClusterFromEnvironment() StormCluster {
	if checkEnv(kerberosPrincipal) && checkEnv(kerberosKeytab) && checkEnv(kerberosDomain) && checkEnv(kerberosConfig) {
		return MakeKerberosStormCluster()
	} else {
		return MakeHttpStormCluster()
	}
}

func checkEnv(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}
