package com.sling.hotel.application.usecase;

import com.sling.hotel.application.ports.in.SaveSearchPort;
import com.sling.hotel.application.ports.out.SearchRepositoryPort;
import com.sling.hotel.domain.model.HotelSearch;

public class SaveSearchUseCase implements SaveSearchPort {

    private final SearchRepositoryPort searchRepository;

    public SaveSearchUseCase(SearchRepositoryPort searchRepository) {
        this.searchRepository = searchRepository;
    }

    @Override
    public void save(HotelSearch search) {
        searchRepository.save(search);
    }
}
