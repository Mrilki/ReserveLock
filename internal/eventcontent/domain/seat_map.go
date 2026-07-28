package domain

type SeatMap struct {
	ID        string
	HallID    string
	Name      string
	Seats     []Seat
	CreatedBy string
}

type Seat struct {
	ID      string
	Section string
	Row     string
	Number  string
}
