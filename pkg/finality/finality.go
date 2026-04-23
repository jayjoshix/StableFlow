// Package finality defines blockchain finality rules — the number of
// confirmations required before a transaction is considered irreversible
// on each supported chain.
package finality

import (
	"fmt"
	"time"
)

// Chain represents a supported blockchain network.
type Chain string

const (
	Ethereum Chain = "ethereum"
	Solana   Chain = "solana"
	Tron     Chain = "tron"
	Polygon  Chain = "polygon"
	BSC      Chain = "bsc"
	Arbitrum Chain = "arbitrum"
	Base     Chain = "base"
	Stellar  Chain = "stellar"
)

// Stablecoin represents a supported stablecoin on a given chain.
type Stablecoin struct {
	Symbol   string // e.g. "USDC", "USDT", "DAI"
	Chain    Chain
	Contract string // on-chain contract/mint address
	Decimals int
}

// Rule encodes the finality parameters for a specific chain.
type Rule struct {
	Chain               Chain
	Confirmations       int           // block confirmations required
	EstimatedBlockTime  time.Duration // average time per block
	EstimatedFinality   time.Duration // Confirmations × EstimatedBlockTime
	SupportsProbability bool          // true if chain uses probabilistic finality
}

// rules is the internal registry of finality rules.
var rules = map[Chain]Rule{
	Ethereum: {
		Chain:               Ethereum,
		Confirmations:       12,
		EstimatedBlockTime:  12 * time.Second,
		EstimatedFinality:   144 * time.Second,
		SupportsProbability: false, // post-merge: uses Casper finality gadget
	},
	Solana: {
		Chain:               Solana,
		Confirmations:       1,
		EstimatedBlockTime:  400 * time.Millisecond,
		EstimatedFinality:   400 * time.Millisecond,
		SupportsProbability: false, // optimistic confirmation
	},
	Tron: {
		Chain:               Tron,
		Confirmations:       19,
		EstimatedBlockTime:  3 * time.Second,
		EstimatedFinality:   57 * time.Second,
		SupportsProbability: false,
	},
	Polygon: {
		Chain:               Polygon,
		Confirmations:       128,
		EstimatedBlockTime:  2 * time.Second,
		EstimatedFinality:   256 * time.Second,
		SupportsProbability: true,
	},
	BSC: {
		Chain:               BSC,
		Confirmations:       15,
		EstimatedBlockTime:  3 * time.Second,
		EstimatedFinality:   45 * time.Second,
		SupportsProbability: true,
	},
	Arbitrum: {
		Chain:               Arbitrum,
		Confirmations:       1,
		EstimatedBlockTime:  250 * time.Millisecond,
		EstimatedFinality:   250 * time.Millisecond,
		SupportsProbability: false,
	},
	Base: {
		Chain:               Base,
		Confirmations:       1,
		EstimatedBlockTime:  2 * time.Second,
		EstimatedFinality:   2 * time.Second,
		SupportsProbability: false,
	},
	Stellar: {
		Chain:               Stellar,
		Confirmations:       1,
		EstimatedBlockTime:  5 * time.Second,
		EstimatedFinality:   5 * time.Second,
		SupportsProbability: false,
	},
}

// GetRule returns the finality rule for the given chain.
func GetRule(c Chain) (Rule, error) {
	r, ok := rules[c]
	if !ok {
		return Rule{}, fmt.Errorf("finality: unsupported chain %q", c)
	}
	return r, nil
}

// IsFinal returns true if the transaction has enough confirmations
// to be considered final on the given chain.
func IsFinal(chain Chain, confirmations int) (bool, error) {
	r, err := GetRule(chain)
	if err != nil {
		return false, err
	}
	return confirmations >= r.Confirmations, nil
}

// Chains returns all supported chain identifiers.
func Chains() []Chain {
	out := make([]Chain, 0, len(rules))
	for c := range rules {
		out = append(out, c)
	}
	return out
}

// ConfirmationsRequired returns the number of confirmations needed for
// the given chain, or an error if the chain is not supported.
func ConfirmationsRequired(c Chain) (int, error) {
	r, err := GetRule(c)
	if err != nil {
		return 0, err
	}
	return r.Confirmations, nil
}
