//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package searcher

import (
	"math"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

type PhraseSearcher struct {
	indexReader  index.IndexReader
	mustSearcher *ConjunctionSearcher
	queryNorm    float64
	currMust     *search.DocumentMatch
	slop         int
	terms        []string
	initialized  bool
}

func NewPhraseSearcher(indexReader index.IndexReader, mustSearcher *ConjunctionSearcher, terms []string) (*PhraseSearcher, error) {
	// build our searcher
	rv := PhraseSearcher{
		indexReader:  indexReader,
		mustSearcher: mustSearcher,
		terms:        terms,
	}
	rv.computeQueryNorm()
	return &rv, nil
}

func (s *PhraseSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	if s.mustSearcher != nil {
		sumOfSquaredWeights += s.mustSearcher.Weight()
	}

	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downstream searchers the norm
	if s.mustSearcher != nil {
		s.mustSearcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *PhraseSearcher) initSearchers(ctx *search.SearchContext) error {
	err := s.advanceNextMust(ctx)
	if err != nil {
		return err
	}

	s.initialized = true
	return nil
}

func (s *PhraseSearcher) advanceNextMust(ctx *search.SearchContext) error {
	var err error

	if s.mustSearcher != nil {
		s.currMust, err = s.mustSearcher.Next(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *PhraseSearcher) Weight() float64 {
	return s.mustSearcher.Weight()
}

func (s *PhraseSearcher) SetQueryNorm(qnorm float64) {
	s.mustSearcher.SetQueryNorm(qnorm)
}

func checkTermLocationsRecursively(tlm search.TermLocationMap, priorLocation *search.Location, terms []string, termidx int, rvtlm search.TermLocationMap) search.TermLocationMap {
	if len(rvtlm) == len(terms) {
		return rvtlm
	}

	nextlocs, ok := tlm[terms[termidx]]
	if !ok {
		return nil
	}

	// we have to copy the original map here because we may have to
	// reuse it.
	origrv := make(search.TermLocationMap, len(rvtlm))
	for k, v := range rvtlm {
		origrv[k] = v
	}
	for _, nextLocation := range nextlocs {
		if nextLocation.Pos == priorLocation.Pos+float64(1) && nextLocation.SameArrayElement(priorLocation) {
			// found a location match for this
			// term. First we add the prior match,
			// then the current one.
			rvtlm.AddLocation(terms[termidx-1], priorLocation)
			rvtlm.AddLocation(terms[termidx], nextLocation)
			ret := checkTermLocationsRecursively(tlm, nextLocation, terms, termidx+1, rvtlm)
			if ret != nil {
				return rvtlm
			}
			// the following terms did not match but
			// we added the locations up to this point
			// already. We have to undo that by re-assigning
			// the original map.
			rvtlm = origrv
		}
	}
	return nil
}

func (s *PhraseSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}

	var rv *search.DocumentMatch
	for s.currMust != nil {
		rvftlm := make(search.FieldTermLocationMap, 0)

		for field, termLocMap := range s.currMust.Locations {
			curlocs, ok := termLocMap[s.terms[0]]
			if !ok {
				continue
			}

			for _, curloc := range curlocs {
				rvtlm := make(search.TermLocationMap)
				rvtlm = checkTermLocationsRecursively(termLocMap, curloc, s.terms, 1, rvtlm)
				if rvtlm == nil {
					continue
				}
				rvftlm[field] = rvtlm
			}
		}

		if len(rvftlm) > 0 {
			// return match
			rv = s.currMust
			rv.Locations = rvftlm
			err := s.advanceNextMust(ctx)
			if err != nil {
				return nil, err
			}
			return rv, nil
		}

		err := s.advanceNextMust(ctx)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (s *PhraseSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}
	var err error
	s.currMust, err = s.mustSearcher.Advance(ctx, ID)
	if err != nil {
		return nil, err
	}
	return s.Next(ctx)
}

func (s *PhraseSearcher) Count() uint64 {
	// for now return a worst case
	return s.mustSearcher.Count()
}

func (s *PhraseSearcher) Close() error {
	if s.mustSearcher != nil {
		err := s.mustSearcher.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *PhraseSearcher) Min() int {
	return 0
}

func (s *PhraseSearcher) DocumentMatchPoolSize() int {
	return s.mustSearcher.DocumentMatchPoolSize() + 1
}
