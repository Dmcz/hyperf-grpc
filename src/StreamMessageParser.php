<?php

declare(strict_types=1);

namespace Hyperf\Grpc;

use InvalidArgumentException;
use LogicException;
use Throwable;
use UnexpectedValueException;

/**
 * Incrementally decodes the messages of one gRPC stream.
 *
 * Create a new instance when opening a new stream, including after reconnecting.
 */
final class StreamMessageParser
{
    public const DEFAULT_MAX_MESSAGE_LENGTH = 4 * 1024 * 1024;

    private string $buffer = '';

    private bool $closed = false;

    /**
     * @param callable|array{class-string, string} $deserialize The decoder accepted by Parser::deserializePayload().
     * @param string $encoding The stream's grpc-encoding value, or identity when absent.
     * @param int $maxMessageLength Maximum compressed and decompressed payload size in bytes.
     */
    public function __construct(
        private readonly mixed $deserialize,
        private readonly string $encoding = 'identity',
        private readonly int $maxMessageLength = self::DEFAULT_MAX_MESSAGE_LENGTH,
    ) {
        if (! in_array($encoding, ['identity', 'gzip'], true)) {
            throw new InvalidArgumentException(sprintf('Unsupported gRPC message encoding: %s', $encoding));
        }
        if ($maxMessageLength <= 0) {
            throw new InvalidArgumentException('The maximum gRPC message length must be greater than zero.');
        }
    }

    /**
     * Returns all complete messages and retains incomplete bytes for the next call.
     * An empty result does not indicate the end of the stream.
     *
     * @return array Decoded messages in wire order.
     */
    public function parseChunk(string $chunk): array
    {
        $this->assertOpen();
        $this->buffer .= $chunk;
        $offset = 0;
        $total = strlen($this->buffer);
        $messages = [];

        try {
            while ($total - $offset >= 5) {
                $flag = ord($this->buffer[$offset]);
                if ($flag !== 0 && $flag !== 1) {
                    throw new UnexpectedValueException(sprintf('Invalid gRPC compression flag: %d', $flag));
                }
                if ($flag === 1 && $this->encoding === 'identity') {
                    throw new UnexpectedValueException('Compressed gRPC message received without a compression encoding.');
                }

                $length = unpack('Nlength', substr($this->buffer, $offset + 1, 4))['length'];
                if ($length > $this->maxMessageLength) {
                    throw new UnexpectedValueException(sprintf(
                        'gRPC message length %d exceeds the maximum of %d bytes.',
                        $length,
                        $this->maxMessageLength
                    ));
                }

                $frameLength = 5 + $length;
                if ($total - $offset < $frameLength) {
                    break;
                }

                $payload = substr($this->buffer, $offset + 5, $length);
                if ($flag === 1) {
                    if (! function_exists('gzdecode')) {
                        throw new UnexpectedValueException('The zlib extension is required to decode gzip messages.');
                    }
                    $payload = @gzdecode($payload, $this->maxMessageLength);
                    if ($payload === false) {
                        throw new UnexpectedValueException(sprintf(
                            'Invalid gzip payload or decompressed gRPC message exceeds the maximum of %d bytes.',
                            $this->maxMessageLength
                        ));
                    }
                }

                $messages[] = Parser::deserializePayload($this->deserialize, $payload);
                $offset += $frameLength;
            }
        } catch (Throwable $exception) {
            // A failed decoder must never be reused for subsequent stream data.
            $this->buffer = '';
            $this->closed = true;
            throw $exception;
        }

        $this->buffer = substr($this->buffer, $offset);

        return $messages;
    }

    /**
     * Call after parsing all data and verifying a successful gRPC stream status.
     * On transport failure or cancellation, discard the parser instead.
     */
    public function finish(): void
    {
        $this->assertOpen();
        $this->closed = true;

        $remaining = strlen($this->buffer);
        $this->buffer = '';
        if ($remaining !== 0) {
            throw new UnexpectedValueException(sprintf('Truncated gRPC message: %d unconsumed bytes at end of stream.', $remaining));
        }
    }

    private function assertOpen(): void
    {
        if ($this->closed) {
            throw new LogicException('The gRPC stream parser is closed; create a new parser for a new stream.');
        }
    }
}
