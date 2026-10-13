// Retained store interfaces from livepeer/go-livepeer/pm/ticketstore.go,
// by Yondon Fu, Nico Vergauwen, and Rafał Leszko. Source history is retained.
package pm

import ethcommon "github.com/ethereum/go-ethereum/common"

// TicketStore is an interface which describes an object capable
// of persisting tickets
type TicketStore interface {
	// SelectEarliestWinningTicket selects the earliest stored winning ticket for a 'payer'
	// which is not yet redeemed
	SelectEarliestWinningTicket(payer ethcommon.Address, minCreationRound int64) (*SignedTicket, error)

	// RemoveWinningTicket removes a ticket
	RemoveWinningTicket(ticket *SignedTicket) error

	// StoreWinningTicket stores a signed ticket
	StoreWinningTicket(ticket *SignedTicket) error

	// WinningTicketCount returns the amount of non-redeemed winning tickets for a payer in the TicketStore
	WinningTicketCount(payer ethcommon.Address, minCreationRound int64) (int, error)
}
