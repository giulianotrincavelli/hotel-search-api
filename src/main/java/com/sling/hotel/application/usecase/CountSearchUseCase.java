package com.sling.hotel.application.usecase;

import com.sling.hotel.application.ports.in.CountSearchPort;
import com.sling.hotel.application.ports.out.SearchRepositoryPort;
import com.sling.hotel.domain.exception.SearchNotFoundException;
import com.sling.hotel.domain.model.HotelSearch;

public class CountSearchUseCase implements CountSearchPort {

    private final SearchRepositoryPort searchRepository;

    public CountSearchUseCase(SearchRepositoryPort searchRepository) {
        this.searchRepository = searchRepository;
    }

    @Override
    public Result count(String searchId) {
        HotelSearch search = searchRepository.findById(searchId)
                .orElseThrow(() -> new SearchNotFoundException(searchId));

        long count = searchRepository.countIdentical(search);
        return new Result(search, count);
    }
}
