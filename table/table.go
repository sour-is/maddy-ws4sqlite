package table

import (
	"context"
	"fmt"

	"github.com/foxcpp/maddy/framework/config"
	"github.com/foxcpp/maddy/framework/module"

	ws4 "github.com/proofrock/ws4sqlite-client-go"
)

type SQL struct {
	modName  string
	instName string

	db     *ws4.Client
	lookup string
	add    string
	list   string
	set    string
	del    string
}

func NewSQL(modName, instName string, _, _ []string) (module.Module, error) {
	return &SQL{
		modName:  modName,
		instName: instName,
	}, nil
}

func (s *SQL) Name() string {
	return s.modName
}

func (s *SQL) InstanceName() string {
	return s.instName
}

func (s *SQL) Init(cfg *config.Map) error {
	var (
		initQueries []string
		url         string
	)
	cfg.StringList("init", false, false, nil, &initQueries)
	cfg.String("url", false, true, "", &url)
	cfg.String("lookup", false, true, "", &s.lookup)

	cfg.String("add", false, false, "", &s.add)
	cfg.String("list", false, false, "", &s.list)
	cfg.String("del", false, false, "", &s.del)
	cfg.String("set", false, false, "", &s.set)
	if _, err := cfg.Process(); err != nil {
		return err
	}

	db, err := ws4.NewClientBuilder().WithURL(url).Build()
	if err != nil {
		return config.NodeErr(cfg.Block, "failed to open db: %v", err)
	}
	s.db = db

	bldr := ws4.NewRequestBuilder()
	for _, init := range initQueries {
		bldr.AddStatement(init)
	}
	req, err := bldr.Build()
	if err != nil {
		return config.NodeErr(cfg.Block, "failed to init db: %v", err)
	}
	_, _, err = db.Send(req)
	if err != nil {
		return config.NodeErr(cfg.Block, "failed to init db: %v", err)
	}

	return nil
}

func (s *SQL) Close() error {
	return nil
}

func (s *SQL) Lookup(ctx context.Context, val string) (string, bool, error) {
	var (
		repl string
	)
	req, err := ws4.NewRequestBuilder().AddQuery(s.lookup).WithValues(map[string]interface{}{"key": val}).Build()
	if err != nil {
		return "", false, fmt.Errorf("%s: lookup %s: %w", s.modName, val, err)
	}
	res, _, err := s.db.Send(req)
	if err != nil {
		return "", false, fmt.Errorf("%s: lookup %s: %w", s.modName, val, err)
	}
	for _, r := range res.Results {
		if !r.Success {
			return "", false, fmt.Errorf("%s: lookup %s: %s", s.modName, val, r.Error)
		}
		for _, s := range r.ResultSet {
			for _, v := range s {
				repl = fmt.Sprint(v)
				return repl, true, nil
			}
		}
	}
	return repl, false, nil
}

func (s *SQL) LookupMulti(ctx context.Context, val string) ([]string, error) {
	var (
		repl []string
	)
	req, err := ws4.NewRequestBuilder().AddQuery(s.lookup).WithValues(map[string]interface{}{"key": val}).Build()
	if err != nil {
		return repl, fmt.Errorf("%s: lookup-multi %s: %w", s.modName, val, err)
	}
	res, _, err := s.db.Send(req)
	if err != nil {
		return repl, fmt.Errorf("%s: lookup-multi %s: %w", s.modName, val, err)
	}
	for _, r := range res.Results {
		if !r.Success {
			return repl, fmt.Errorf("%s: lookup-multi %s: %v", s.modName, val, r.Error)
		}
		for _, s := range r.ResultSet {
			for _, v := range s {
				repl = append(repl, fmt.Sprint(v))
				return repl, nil
			}
		}
	}
	return repl, nil
}

func (s *SQL) Keys() ([]string, error) {
	if s.list == "" {
		return nil, fmt.Errorf("%s: table is not mutable (no 'list' query)", s.modName)
	}

	var (
		repl []string
	)
	req, err := ws4.NewRequestBuilder().AddQuery(s.lookup).Build()
	if err != nil {
		return repl, fmt.Errorf("%s: list: %w", s.modName, err)
	}
	res, _, err := s.db.Send(req)
	if err != nil {
		return repl, fmt.Errorf("%s: list: %w", s.modName, err)
	}
	for _, r := range res.Results {
		if !r.Success {
			return repl, fmt.Errorf("%s: list: %v", s.modName, r.Error)
		}
		for _, s := range r.ResultSet {
			for _, v := range s {
				repl = append(repl, fmt.Sprint(v))
				return repl, nil
			}
		}
	}
	return repl, nil
}

func (s *SQL) RemoveKey(k string) error {
	if s.del == "" {
		return fmt.Errorf("%s: table is not mutable (no 'del' query)", s.modName)
	}

	req, err := ws4.NewRequestBuilder().AddStatement(s.del).WithValues(map[string]interface{}{"key": k}).Build()
	if err != nil {
		return fmt.Errorf("%s: del %s: %w", s.modName, k, err)
	}
	_, _, err = s.db.Send(req)
	if err != nil {
		return fmt.Errorf("%s: del %s: %w", s.modName, k, err)
	}
	if err != nil {
		return fmt.Errorf("%s: del %s: %w", s.modName, k, err)
	}
	return nil
}

func (s *SQL) SetKey(k, v string) error {
	if s.set == "" {
		return fmt.Errorf("%s: table is not mutable (no 'set' query)", s.modName)
	}
	if s.add == "" {
		return fmt.Errorf("%s: table is not mutable (no 'add' query)", s.modName)
	}

	req, err := ws4.NewRequestBuilder().AddStatement(s.set).WithValues(map[string]interface{}{"key": k, "value": v}).Build()
	if err != nil {
		return fmt.Errorf("%s: set %s: %w", s.modName, k, err)
	}
	res, _, err := s.db.Send(req)
	if err != nil {
		return fmt.Errorf("%s: set %s: %w", s.modName, k, err)
	}

	for _, r := range res.Results {
		if *r.RowsUpdated == 0 {
			req, err := ws4.NewRequestBuilder().AddStatement(s.add).WithValues(map[string]interface{}{"key": k, "value": v}).Build()
			if err != nil {
				return fmt.Errorf("%s: add %s: %w", s.modName, k, err)
			}
			_, _, err = s.db.Send(req)
			if err != nil {
				return fmt.Errorf("%s: add %s: %w", s.modName, k, err)
			}
		}
		break //lint:ignore SA4004 only loop once
	}

	return nil
}

func init() {
	module.Register("table.ws4sql_query", NewSQL)
}
