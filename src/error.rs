#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid tree format")]
    InvalidTreeFormat(Vec<u8>),

    #[error("corrupted file: {0}")]
    CorruptedFile(&'static str),

    #[error("invalid compression type: {0}")]
    InvalidCompression(u8),

    #[error("incorrect block length, expected {0}, got {1}")]
    IncorrectBlockLength(u32, u32),

    #[error("expected PosLen entry")]
    PosLenEntryRequired,

    #[error("invalid entry tag {0}")]
    InvalidEntryTag(u8),

    #[error("internal buffer conversion error: {0}")]
    SliceConversion(#[from] std::array::TryFromSliceError),

    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("end of file reached")]
    EndOfFile,

    #[error("incomplete entry - {0}")]
    IncompleteEntry(std::io::Error),

    #[error("out-of-order write")]
    OutOfOrderWrite,

    #[error("bloom filter too large")]
    BloomFilterTooLarge,

    #[error("bloom filter did not serialize or deserialize: {0}")]
    BloomFilterCorrupted(#[from] postcard::Error),

    #[error("snappy decompression error: {0}")]
    SnappyDecompression(#[from] snap::Error),

    #[error("snappy compression error: {0}")]
    SnappyCompression(std::io::Error),

    #[error("lz4 compression error: {0}")]
    Lz4Compression(#[from] lz4_flex::frame::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
