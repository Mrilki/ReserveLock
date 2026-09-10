package mongodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const hallsCollection = "halls"

type HallRepository struct {
	collection *mongo.Collection
}

func NewHallRepository(
	database *mongo.Database,
) *HallRepository {
	return &HallRepository{
		collection: database.Collection(hallsCollection),
	}
}

func (r *HallRepository) Create(
	ctx context.Context,
	hall domain.Hall,
) error {
	_, err := r.collection.InsertOne(
		ctx,
		toHallDocument(hall),
	)
	if err != nil {
		return fmt.Errorf(
			"insert hall: %w",
			err,
		)
	}

	return nil
}

func (r *HallRepository) GetByID(
	ctx context.Context,
	id string,
) (domain.Hall, error) {
	var document hallDocument

	err := r.collection.FindOne(
		ctx,
		bson.M{
			"_id": id,
		},
	).Decode(&document)

	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return domain.Hall{},
			domain.ErrHallNotFound

	case err != nil:
		return domain.Hall{}, fmt.Errorf(
			"find hall by id: %w",
			err,
		)
	}

	return document.toDomain(), nil
}
