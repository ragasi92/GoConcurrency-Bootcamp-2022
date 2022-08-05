package repositories

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"GoConcurrency-Bootcamp-2022/models"
)

type LocalStorage struct{}

const filePath = "resources/pokemons.csv"

func (l LocalStorage) Write(pokemons []models.Pokemon) error {
	file, fErr := os.Create(filePath)

	if fErr != nil {
		return fErr
	}

	defer file.Close()

	w := csv.NewWriter(file)
	records := buildRecords(pokemons)
	if err := w.WriteAll(records); err != nil {
		return err
	}

	return nil
}

func (l LocalStorage) Read(ctx context.Context) (<-chan models.Pokemon, <-chan error, error) {
	file, fErr := os.Open(filePath)
	if fErr != nil {
		return nil, nil, fErr
	}
	var wg sync.WaitGroup
	wg.Add(1)

	out, errc := readRecords(ctx, file, wg)

	go func() {
		wg.Wait()
		file.Close()
	}()
	return out, errc, nil
}

func readRecords(ctx context.Context, r io.Reader, wg sync.WaitGroup) (<-chan models.Pokemon, <-chan error) {
	out := make(chan models.Pokemon)
	errc := make(chan error, 1)
	go func(ctx context.Context) {
		defer close(errc)
		defer close(out)
		reader := csv.NewReader(r)
		_, err := reader.Read()
		if err != nil {
			log.Println("err", err)
			return
		}
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errc <- err
				return
			}

			pokemon, err := parseCSVData(record)
			if err != nil {
				errc <- err
				return
			}

			select {
			case <-ctx.Done():
				return
			default:
				out <- *pokemon
			}

		}
		wg.Done()
	}(ctx)
	return out, errc

}

func buildRecords(pokemons []models.Pokemon) [][]string {
	headers := []string{"id", "name", "height", "weight", "flat_abilities"}
	records := [][]string{headers}
	for _, p := range pokemons {
		record := fmt.Sprintf("%d,%s,%d,%d,%s",
			p.ID,
			p.Name,
			p.Height,
			p.Weight,
			p.FlatAbilityURLs)
		records = append(records, strings.Split(record, ","))
	}

	return records
}

func parseCSVData(record []string) (*models.Pokemon, error) {

	id, err := strconv.Atoi(record[0])
	if err != nil {
		return nil, err
	}

	height, err := strconv.Atoi(record[2])
	if err != nil {
		return nil, err
	}

	weight, err := strconv.Atoi(record[3])
	if err != nil {
		return nil, err
	}

	pokemon := &models.Pokemon{
		ID:              id,
		Name:            record[1],
		Height:          height,
		Weight:          weight,
		Abilities:       nil,
		FlatAbilityURLs: record[4],
		EffectEntries:   nil,
	}

	return pokemon, nil
}
