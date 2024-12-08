<?php

namespace Yangweijie\WebmanParquetLog;

use Flow\Parquet\Options;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{data_frame, from_array, to_array, overwrite, schema, str_schema};

class Logger {

    public $logFile;
    public $schema;


    public function __construct(string $filePath) {
        $this->logFile = $filePath;
        $this->schema  = schema(
            str_schema('traceId', $nullable = false),
            str_schema('requestId', $nullable = false),
            str_schema('channel', $nullable = false),
            str_schema('context', $nullable = false),
            str_schema('level', $nullable = false),
            str_schema('message', $nullable = false),
            str_schema('datetime', $nullable = false),
        );
    }

    public function log(array $data) {
        $info = [];
        $destination = $this->logFile;

        foreach ($data as $type => $msges) {
            foreach ($msges as $msg) {
                $info[] = $msg;
            }
        }

        var_dump($data);

        $old_logs = [];
        if(is_file($destination)){
            data_frame()
                ->read(from_parquet(
                    $destination,
                ))
                ->collect()
                ->write(to_array($old_logs))
                ->run();
        }
        data_frame()
            ->read(from_array(array_merge($old_logs, $info), $this->schema))
            ->collect()
            ->saveMode(overwrite())
            ->write(to_parquet($destination))
            ->run();

        return true;
    }

    public function list($params = []){
        $destination = $this->logFile;
        $all = [];
        if($destination && is_file($destination)){
            data_frame()
                ->read(from_parquet(
                    $destination,
                ))
                ->collect()
                ->write(to_array($all))
                ->run();
            var_dump('1111');
            var_dump($all);
            $all = array_reverse($all);
            extract($params);
            if(isset($map) && $map){
                array_filter($all, function($value)use($map){
                    $find = false;
                    $find_result = [];
                    foreach ($map as $k=>$v){
                        if(str_contains($value[$k], $v)){
                            $find_result[] = $k;
                        }
                    }
                    $find = count($find_result) == count($map);
                    return $find;
                });
            }
            return [
                'count'=>count($all),
                'data'=>array_slice($all, ($page - 1) * $limit, $limit),
            ];
        }else{
            return [
                'count'=>0,
                'data'=>[],
            ];
        }
    }
}
