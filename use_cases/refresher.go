package use_cases

import (
	"context"
	"strings"
	"sync"

	"GoConcurrency-Bootcamp-2022/models"
)

type reader interface {
	Read(ctx context.Context) (<-chan models.Pokemon, <-chan error, error)
}

type saver interface {
	Save(context.Context, models.Pokemon) error
}

type fetcher interface {
	FetchAbility(string) (models.Ability, error)
}

type Refresher struct {
	reader
	saver
	fetcher
}

type AbilityResult struct {
	Error   error
	Pokemon *models.Ability
}

func NewRefresher(reader reader, saver saver, fetcher fetcher) Refresher {
	return Refresher{reader, saver, fetcher}
}

func (r Refresher) Refresh(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var errcList []<-chan error
	pokeChan, errc, err := r.Read(ctx)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	pokeChan2, errc := r.buildPokemon(ctx, pokeChan)
	errcList = append(errcList, errc)

	errc, err = r.savePokemon(ctx, pokeChan2)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	return WaitForPipeline(errcList...)
}

func WaitForPipeline(errs ...<-chan error) error {
	errc := mergeErrorChans(errs...)
	for err := range errc {
		if err != nil {
			return err
		}
	}
	return nil
}

func (r Refresher) buildPokemon(ctx context.Context, in <-chan models.Pokemon) (<-chan models.Pokemon, <-chan error) {

	wk1, errc1 := r.pokeWorker(ctx, in)
	wk2, errc2 := r.pokeWorker(ctx, in)
	wk3, errc3 := r.pokeWorker(ctx, in)

	pokemonChan := fanIn(wk1, wk2, wk3)
	errcm := mergeErrorChans(errc1, errc2, errc3)
	return pokemonChan, errcm

}

func (r Refresher) savePokemon(ctx context.Context, in <-chan models.Pokemon) (<-chan error, error) {
	errc := make(chan error, 1)
	go func() {
		defer close(errc)
		for p := range in {
			if err := r.Save(ctx, p); err != nil {
				errc <- err
				return
			}
		}
	}()
	return errc, nil
}

func (r Refresher) pokeWorker(ctx context.Context, in <-chan models.Pokemon) (<-chan models.Pokemon, <-chan error) {

	out := make(chan models.Pokemon)
	errc := make(chan error, 1)

	go func() {
		var wg sync.WaitGroup

		for pokemon := range in {
			wg.Add(1)

			go func(pokemon models.Pokemon) {
				defer wg.Done()

				var abilities []string
				urls := strings.Split(pokemon.FlatAbilityURLs, "|")

				for _, url := range urls {
					ability, err := r.FetchAbility(url)
					if err != nil {
						errc <- err
						return
					}

					for _, ee := range ability.EffectEntries {
						abilities = append(abilities, ee.Effect)
					}
				}

				pokemon.EffectEntries = abilities

				select {
				case <-ctx.Done():
					return
				default:
					out <- pokemon
				}
			}(pokemon)
		}

		go func() {
			wg.Wait()
			close(out)
			close(errc)
		}()

	}()
	return out, errc
}

func fanIn(inputs ...<-chan models.Pokemon) <-chan models.Pokemon {
	var wg sync.WaitGroup
	out := make(chan models.Pokemon)
	wg.Add(len(inputs))

	for _, in := range inputs {
		go func(ch <-chan models.Pokemon) {

			for {
				value, ok := <-ch
				if !ok {
					wg.Done()
					break
				}

				out <- value
			}

		}(in)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func mergeErrorChans(inputs ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	out := make(chan error)
	wg.Add(len(inputs))

	for _, in := range inputs {
		go func(ch <-chan error) {

			for {
				value, ok := <-ch
				if !ok {
					wg.Done()
					break
				}

				out <- value
			}

		}(in)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
