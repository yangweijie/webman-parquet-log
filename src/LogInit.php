<?php

namespace Yangweijie\WebmanParquetLog;
use PUGX\Shortid\Shortid;
use Webman\Http\Response;
use Webman\Http\Request;
use Webman\MiddlewareInterface;


class LogInit implements MiddlewareInterface
{
    public function process(Request $request, callable $handler) : Response
    {
        $request->traceId = (string)Shortid::generate();
        $response = $handler($request); // 继续向洋葱芯穿越，直至执行控制器得到响应
        return $response;
    }
}