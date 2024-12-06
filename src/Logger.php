<?php

namespace Yangweijie\WebmanParquetLog;

use Flow\ETL\DataFrame;
use Flow\ETL\Adapter\Parquet\ParquetWriter;
use Flow\ETL\Row;
use Flow\ETL\Rows;

class Logger {
    private $writer;

    public function __construct(string $filePath) {
        $this->writer = new ParquetWriter($filePath);
    }

    public function log(array $data) {
        $rows = new Rows(
            ...array_map(fn($item) => Row::create($item), $data)
        );
        $dataFrame = new DataFrame($rows);
        $this->writer->write($dataFrame);
    }
}
