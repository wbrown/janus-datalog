package parser

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/edn"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ParseRules parses a rule set from EDN, a vector of rules each of the form
// [(name ?args...) clause...], and validates and stratifies it through
// query.NewRuleSet.
func ParseRules(input string) (*query.RuleSet, error) {
	node, err := edn.Parse(input)
	if err != nil {
		return nil, fmt.Errorf("EDN parse error: %w", err)
	}
	if node.Type != edn.NodeVector {
		return nil, fmt.Errorf("rule set must be a vector of rules, got %v", node.Type)
	}
	rules := make([]*query.Rule, 0, len(node.Nodes))
	for i := range node.Nodes {
		rule, err := parseRule(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i+1, err)
		}
		rules = append(rules, rule)
	}
	return query.NewRuleSet(rules)
}

// parseRule parses one rule vector: a head list naming the predicate and its
// variables, a leading vector among them the required run the caller binds,
// then at least one body clause.
func parseRule(node *edn.Node) (*query.Rule, error) {
	if node.Type != edn.NodeVector || len(node.Nodes) == 0 {
		return nil, fmt.Errorf("a rule is a vector [(name ?args...) clause...], got %v", node.Type)
	}
	head := &node.Nodes[0]
	if head.Type != edn.NodeList || len(head.Nodes) == 0 {
		return nil, fmt.Errorf("rule head must be a list (name ?args...), got %v", head.Type)
	}
	if head.Nodes[0].Type != edn.NodeSymbol {
		return nil, fmt.Errorf("rule name must be a symbol, got %v", head.Nodes[0].Type)
	}
	name := datalog.NewSymbol(head.Nodes[0].Value)
	if name.IsVariable() {
		return nil, fmt.Errorf("rule name must be a symbol, not the variable %s", head.Nodes[0].Value)
	}
	rule := &query.Rule{Name: name}
	headVariable := func(arg *edn.Node) (query.Symbol, error) {
		if arg.Type != edn.NodeSymbol {
			return nil, fmt.Errorf("rule %s: head argument must be a variable, got %v", name, arg.Type)
		}
		sym := datalog.NewSymbol(arg.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("rule %s: head argument must be a variable, got %s", name, arg.Value)
		}
		return sym, nil
	}
	for i := 1; i < len(head.Nodes); i++ {
		arg := &head.Nodes[i]
		if arg.Type == edn.NodeVector {
			if i != 1 {
				return nil, fmt.Errorf("rule %s: the required run [?x ...] leads the head: (name [?x ...] ?y ...)", name)
			}
			if len(arg.Nodes) == 0 {
				return nil, fmt.Errorf("rule %s: the required run names at least one variable", name)
			}
			for j := range arg.Nodes {
				sym, err := headVariable(&arg.Nodes[j])
				if err != nil {
					return nil, fmt.Errorf("rule %s: the required run holds %v, not a variable", name, arg.Nodes[j].Value)
				}
				rule.Required = append(rule.Required, sym)
				rule.Head = append(rule.Head, sym)
			}
			continue
		}
		sym, err := headVariable(arg)
		if err != nil {
			return nil, err
		}
		rule.Head = append(rule.Head, sym)
	}
	if len(node.Nodes) == 1 {
		return nil, fmt.Errorf("rule %s needs at least one body clause", name)
	}
	for i := 1; i < len(node.Nodes); i++ {
		clause, err := parseWhereElement(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("rule %s: body clause %d: %w", name, i, err)
		}
		rule.Body = append(rule.Body, clause)
	}
	return rule, nil
}

// parseRuleInvocation parses (name args...) in a :where: a list whose head
// symbol names no reserved form. Each argument is a variable, a constant or
// a blank.
func parseRuleInvocation(node *edn.Node) (*query.RuleInvocation, error) {
	head := &node.Nodes[0]
	name := datalog.NewSymbol(head.Value)
	if name.IsVariable() {
		return nil, fmt.Errorf("rule invocation name must be a symbol, not the variable %s (a data pattern is a vector [e a v])", head.Value)
	}
	inv := &query.RuleInvocation{Name: name, Args: make([]query.PatternElement, 0, len(node.Nodes)-1)}
	for i := 1; i < len(node.Nodes); i++ {
		arg, err := parsePatternElement(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("rule invocation %s argument %d: %w", name, i, err)
		}
		inv.Args = append(inv.Args, arg)
	}
	return inv, nil
}
