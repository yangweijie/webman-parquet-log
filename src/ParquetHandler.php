<?php

namespace Yangweijie\WebmanParquetLog;

use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Logger as MonologLogger;
use PUGX\Shortid\Shortid;

class ParquetHandler extends AbstractProcessingHandler
{
    private $parquetLogger;

    public function __construct(string $filePath, $maxFiles = 7, $level = MonologLogger::DEBUG, bool $bubble = true)
    {
        $this->parquetLogger = new Logger($filePath);
        parent::__construct($level, $bubble);
    }

    protected function write(\Monolog\LogRecord $record): void
    {
        $request = request();
        $requestId = $request->traceId?? (string)Shortid::generate();
        $traceId = $requestId;

        $this->parquetLogger->log([
            [
                'channel' => $record['channel'],
                'message' => $record['message'],
                'context' => json_encode($record['context']),
                'level' => $record['level_name'],
                'datetime' => self::getTimestamp()->format('Y-m-d H:i:s.v'),
                'requestId' => $requestId,
                'traceId' => $traceId,
            ]
        ]);
    }

    public static function getTimestamp(){
        $originalTime = microtime(true);
        $micro = sprintf("%06d", ($originalTime - floor($originalTime)) * 1000000);
        $date = new \DateTime(date('Y-m-d H:i:s.'.$micro, (int)$originalTime));
        return $date;
    }
}
