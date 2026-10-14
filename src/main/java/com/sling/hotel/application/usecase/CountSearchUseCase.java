package com.sling.hotel.application.usecase;

import com.sling.hotel.application.ports.in.CountSearchPort;
import com.sling.hotel.application.ports.out.SearchRepositoryPort;
import com.sling.hotel.domain.exception.SearchNotFoundException;

public class CountSearchUseCase implements CountSearchPort {

    private final SearchRepositoryPort searchRepository;

    public CountSearchUseCase(SearchRepositoryPort searchRepository) {
        this.searchRepository = searchRepository;
    }

    @Override
    public Result count(String searchId) {
        return searchRepository.findWithCount(searchId)
                .map(found -> new Result(found.search(), found.count()))
                .orElseThrow(() -> new SearchNotFoundException(searchId));
    }
}
