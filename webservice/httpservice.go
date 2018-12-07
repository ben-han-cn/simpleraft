package webservice

import (
	"net/http"

	"github.com/ben-han-cn/simpleraft"
	"github.com/julienschmidt/httprouter"
)

type HttpService struct {
	router *httprouter.Router
	node   *simpleraft.Node
	addr   string
}

func Run(node *simpleraft.Node, addr string) error {
	return New(node, addr).Run()
}

func New(node *simpleraft.Node, addr string) *HttpService {
	return &HttpService{
		router: httprouter.New(),
		node:   node,
		addr:   addr,
	}
}

func (s *HttpService) RegisterHandler(method, path string, handle httprouter.Handle) {
	s.router.Handle(method, path, handle)
}

func (s *HttpService) Run() error {
	s.router.GET("/cluster/members", s.getMembers)
	s.router.POST("/cluster/members", s.addMember)
	return http.ListenAndServe(s.addr, s.router)
}

func (s *HttpService) getMembers(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var members []string
	role, ok := r.URL.Query()["role"]
	if ok {
		if len(role) != 1 || role[0] != "leader" {
			EncodeResult(w, Failed("only support get leader"))
			return
		}
		members = []string{s.node.Leader()}
	} else {
		members, _ = s.node.Peers()
	}

	EncodeResult(w, SucceedWithResult(&struct {
		Members []string `json:"members"`
	}{
		members,
	}))
}

func (s *HttpService) addMember(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if s.node.IsLeader() == false {
		EncodeResult(w, Failed("not leader"))
		return
	}

	param := struct {
		Addr string `json:"addr"`
	}{}
	err := DecodeRequestBody(r, &param)
	if err != nil {
		EncodeResult(w, Failed("request body fmt isn't valid"))
		return
	}

	if err := s.node.Join(param.Addr); err == nil {
		EncodeResult(w, Succeed())
	} else {
		EncodeResult(w, Failed(err.Error()))
	}
}
