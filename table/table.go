package table

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/foxcpp/maddy/framework/config"
	"github.com/foxcpp/maddy/framework/module"
	lru "github.com/hashicorp/golang-lru"

	ws4 "github.com/jonlundy/ws4sqlite-client-go"
)

type Table struct {
	modName  string
	instName string

	lookup string
	add    string
	list   string
	set    string
	del    string

	sql querier
}

func NewTable(modName, instName string, _, _ []string) (module.Module, error) {
	return &Table{
		modName:  modName,
		instName: instName,
	}, nil
}

func (s *Table) Name() string {
	return s.modName
}

func (s *Table) InstanceName() string {
	return s.instName
}

func (s *Table) Init(cfg *config.Map) error {
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

	var cacheSize int64
	cfg.Int64("cache_size", false, false, 10_000, &cacheSize)

	if _, err := cfg.Process(); err != nil {
		return err
	}

	db, err := ws4.NewClientBuilder().WithURL(url).Build()
	if err != nil {
		return config.NodeErr(cfg.Block, "failed to open db: %v", err)
	}
	s.sql = &wsql{db}

	if cacheSize > 0 {
		c, err := lru.New(int(cacheSize))
		if err != nil {
			return err
		}
		s.sql = &cache{sql: s.sql, c: c}
	}

	if len(initQueries) > 0 {

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
	}

	return nil
}

func (s *Table) Close() error {
	return s.sql.Close()
}

func (s *Table) Lookup(ctx context.Context, val string) (string, bool, error) {
	req, err := ws4.NewRequestBuilder().AddQuery(s.lookup).WithValues(map[string]interface{}{"key": val}).Build()
	if err != nil {
		return "", false, fmt.Errorf("%s: lookup %s: %w", s.modName, val, err)
	}
	lis, err := s.sql.query(ctx, "L"+val, req)
	if err != nil {
		err = fmt.Errorf("%s: lookup %s: %w", s.modName, val, err)
	}

	if len(lis) == 0 {
		return "", false, err
	}
	return lis[0], true, err
}

func (s *Table) LookupMulti(ctx context.Context, val string) ([]string, error) {
	req, err := ws4.NewRequestBuilder().AddQuery(s.lookup).WithValues(map[string]interface{}{"key": val}).Build()
	if err != nil {
		return nil, fmt.Errorf("%s: lookup-multi %s: %w", s.modName, val, err)
	}
	lis, err := s.sql.query(ctx, "M"+val, req)
	if err != nil {
		return lis, fmt.Errorf("%s: lookup-multi: %v %w ", s.modName, lis, err)

	}
	return lis, nil
}

func (s *Table) Keys() ([]string, error) {
	if s.list == "" {
		return nil, fmt.Errorf("%s: table is not mutable (no 'list' query)", s.modName)
	}
	req, err := ws4.NewRequestBuilder().AddQuery(s.list).Build()
	if err != nil {
		return nil, fmt.Errorf("%s: list: %w", s.modName, err)
	}
	lis, err := s.sql.query(context.TODO(), "K", req)
	return lis, fmt.Errorf("%s: list: %w", s.modName, err)
}

func (s *Table) RemoveKey(k string) error {
	if s.del == "" {
		return fmt.Errorf("%s: table is not mutable (no 'del' query)", s.modName)
	}

	req, err := ws4.NewRequestBuilder().AddStatement(s.del).WithValues(map[string]interface{}{"key": k}).Build()
	if err != nil {
		return fmt.Errorf("%s: del %s: %w", s.modName, k, err)
	}
	_, err = s.sql.exec(context.TODO(), req)
	if err != nil {
		return fmt.Errorf("%s: del %s: %w", s.modName, k, err)
	}
	if err != nil {
		return fmt.Errorf("%s: del %s: %w", s.modName, k, err)
	}
	return nil
}

func (s *Table) SetKey(k, v string) error {
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
	res, err := s.sql.exec(context.TODO(), req)
	if err != nil {
		return fmt.Errorf("%s: set %s: %w", s.modName, k, err)
	}

	for _, r := range res.Results {
		if *r.RowsUpdated == 0 {
			req, err := ws4.NewRequestBuilder().AddStatement(s.add).WithValues(map[string]interface{}{"key": k, "value": v}).Build()
			if err != nil {
				return fmt.Errorf("%s: add %s: %w", s.modName, k, err)
			}
			_, err = s.sql.exec(context.TODO(), req)
			if err != nil {
				return fmt.Errorf("%s: add %s: %w", s.modName, k, err)
			}
		}
		break //lint:ignore SA4004 only loop once
	}

	return nil
}

type querier interface {
	io.Closer
	query(context.Context, string, *ws4.Request) ([]string, error)
	exec(context.Context, *ws4.Request) (*ws4.Response, error)
}

type wsql struct {
	db *ws4.Client
}

func (s *wsql) Close() error {
	return nil
}

func (s *wsql) query(ctx context.Context, key string, req *ws4.Request) ([]string, error) {
	var (
		repl []string
	)
	res, _, err := s.db.SendWithContext(ctx, req)
	if err != nil {
		return repl, err
	}
	for _, r := range res.Results {
		if !r.Success {
			return repl, err
		}
		for _, s := range r.ResultSet {
			for _, v := range s {
				repl = append(repl, fmt.Sprint(v))
				break
			}
		}
	}
	return repl, nil
}
func (s *wsql) exec(ctx context.Context, req *ws4.Request) (*ws4.Response, error) {
	res, _, err := s.db.SendWithContext(ctx, req)
	return res, err
}

type cache struct {
	sql querier

	c *lru.Cache
}

func (s *cache) Close() error {
	s.c.Purge()
	return s.sql.Close()
}

func (s *cache) exec(ctx context.Context, req *ws4.Request) (*ws4.Response, error) {
	return s.sql.exec(ctx, req)
}
func (s *cache) query(ctx context.Context, key string, req *ws4.Request) ([]string, error) {
	if s.c == nil {
		return s.sql.query(ctx, key, req)
	}

	type cacheKey string
	type cacheValue struct {
		lis       []string
		lastValid time.Time
	}

	refresh := func(ctx context.Context, cancel func()) (*cacheValue, error) {
		if cancel != nil {
			defer cancel()
		}

		var err error
		v := &cacheValue{lastValid: time.Now()}
		v.lis, err = s.sql.query(ctx, key, req)
		if err != nil {
			s.c.Add(key, v)
		}
		return v, err
	}

	if v, ok := s.c.Get(cacheKey(key)); ok {
		if lis, ok := v.(cacheValue); ok {
			if time.Now().After(lis.lastValid.Add(30 * time.Minute)) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				go refresh(ctx, cancel)
			}

			return lis.lis, nil
		}

		return nil, nil
	}

	v, err := refresh(ctx, nil)
	return v.lis, err
}

func init() {
	module.Register("table.ws4sql_query", NewTable)
}
