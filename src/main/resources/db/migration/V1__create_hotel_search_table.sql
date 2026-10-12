CREATE TABLE hotel_search (
    search_id   VARCHAR2(36 CHAR)  NOT NULL,
    hotel_id    VARCHAR2(50 CHAR)  NOT NULL,
    check_in    DATE               NOT NULL,
    check_out   DATE               NOT NULL,
    ages        VARCHAR2(200 CHAR) NOT NULL,
    CONSTRAINT pk_hotel_search PRIMARY KEY (search_id)
);

-- Index for counting identical searches.
-- Uses exact match on hotelId, checkIn, checkOut and ages (comma-separated string).
CREATE INDEX idx_hotel_search_identity ON hotel_search (hotel_id, check_in, check_out, ages);
