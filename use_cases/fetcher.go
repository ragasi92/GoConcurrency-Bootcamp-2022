package use_cases

import (
	"context"
	"strings"
	"sync"

	"GoConcurrency-Bootcamp-2022/models"
)

type api interface {
	FetchPokemon(id int) (models.Pokemon, error)
}

type writer interface {
	Write(pokemons []models.Pokemon) error
}

type Fetcher struct {
	api     api
	storage writer
}

func NewFetcher(api api, storage writer) Fetcher {
	return Fetcher{api, storage}
}

func (f Fetcher) Fetch(ctx context.Context, from, to int) error {
	var pokemons []models.Pokemon
	ctx, cancel := context.WithCancel(ctx)

	pokeChannel := f.pokemonGeneretor(ctx, from, to)

	for pokeResult := range pokeChannel {
		if pokeResult.Error != nil && pokeResult.Pokemon == nil {
			cancel()
			return pokeResult.Error
		}
		pokemons = append(pokemons, *pokeResult.Pokemon)
	}

	return f.storage.Write(pokemons)
}

func (f Fetcher) pokemonGeneretor(ctx context.Context, from, to int) <-chan models.PokemonResult {
	pokemonChan := make(chan models.PokemonResult)
	wg := sync.WaitGroup{}

	for id := from; id <= to; id++ {
		wg.Add(1)
		go func(ctx context.Context, f Fetcher, id int) {
			defer wg.Done()
			var result models.PokemonResult
			pokemon, err := f.api.FetchPokemon(id)
			result = models.PokemonResult{Pokemon: &pokemon, Error: err}
			if err != nil {
				result.Pokemon = nil

			}

			if result.Pokemon != nil {
				var flatAbilities []string
				for _, t := range pokemon.Abilities {
					flatAbilities = append(flatAbilities, t.Ability.URL)
				}
				result.Pokemon.FlatAbilityURLs = strings.Join(flatAbilities, "|")
				result.Error = nil
			}

			select {
			case <-ctx.Done():
				return
			case pokemonChan <- result:
			}
		}(ctx, f, id)

	}

	go func() {
		wg.Wait()
		close(pokemonChan)
	}()

	return pokemonChan

}
