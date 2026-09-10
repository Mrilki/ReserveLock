package mongodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const seatMapsCollection = "seat_maps"

type SeatMapRepository struct {
	collection *mongo.Collection
}

func NewSeatMapRepository(database *mongo.Database) *SeatMapRepository {
	return &SeatMapRepository{
		collection: database.Collection(seatMapsCollection),
	}
}

func (r *SeatMapRepository) Create(
	ctx context.Context,
	seatMap domain.SeatMap,
) error {
	_, err := r.collection.InsertOne(
		ctx,
		toSeatMapDocument(seatMap),
	)
	if err != nil {
		return fmt.Errorf(
			"insert seat map: %w",
			err,
		)
	}

	return nil
}

func (r *SeatMapRepository) GetByID(
	ctx context.Context,
	id string,
) (domain.SeatMap, error) {
	var document seatMapDocument

	err := r.collection.FindOne(
		ctx,
		bson.M{
			"_id": id,
		},
	).Decode(&document)

	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return domain.SeatMap{},
			domain.ErrSeatMapNotFound

	case err != nil:
		return domain.SeatMap{}, fmt.Errorf(
			"find seat map by id: %w",
			err,
		)
	}

	return document.toDomain(), nil
}
