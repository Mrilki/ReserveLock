package mongodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const venuesCollection = "venues"

type VenueRepository struct {
	collection *mongo.Collection
}

func NewVenueRepository(database *mongo.Database) *VenueRepository {
	return &VenueRepository{collection: database.Collection(venuesCollection)}
}

func (r *VenueRepository) Create(ctx context.Context, venue domain.Venue) error {
	_, err := r.collection.InsertOne(ctx, toVenueDocument(venue))
	if err != nil {
		return fmt.Errorf("insert venue: %w", err)
	}
	return nil
}

func (r *VenueRepository) GetByID(ctx context.Context, id string) (domain.Venue, error) {
	var document venueDocument
	err := r.collection.FindOne(
		ctx,
		bson.M{"_id": id},
	).Decode(&document)

	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return domain.Venue{}, domain.ErrVenueNotFound

	case err != nil:
		return domain.Venue{}, fmt.Errorf("find venue by id: %w", err)
	}
	return document.toDomain(), nil
}
