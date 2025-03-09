#include "../src/darray.h"
#include "../src/encoding.h"
#include "../src/timeseries.h"
#include "test_helpers.h"
#include "tests.h"
#include <stdio.h>
#include <stdlib.h>

// For testing purposes, we'll define a maximum buffer size
#define MAX_BUFFER_SIZE  4096
#define MAX_MESSAGE_SIZE 1024

static int test_encode_request_simple(void)
{
    TEST_HEADER;

    uint8_t buffer[MAX_BUFFER_SIZE] = {0};
    char query[]                    = "CREATE db";
    request_t req                   = {.length = strlen(query)};

    strncpy(req.query, query, req.length);

    ssize_t result     = encode_request(&req, buffer);

    // Expected output: $9\r\nCREATE db\r\n
    uint8_t expected[] = {'$', '9', '\r', '\n', 'C', 'R',  'E', 'A',
                          'T', 'E', ' ',  'd',  'b', '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);
    ASSERT_TRUE(memcmp(expected, buffer, result) == 0,
                " FAIL: encoded buffer doesn't match expected\n");

    TEST_FOOTER;
    return 0;
}

static int test_encode_request_empty(void)
{
    TEST_HEADER;

    uint8_t buffer[MAX_BUFFER_SIZE] = {0};
    request_t req                   = {.query = "", .length = 0};

    ssize_t result                  = encode_request(&req, buffer);

    // Expected output: $0\r\n\r\n
    uint8_t expected[]              = {'$', '0', '\r', '\n', '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);
    ASSERT_TRUE(memcmp(expected, buffer, result) == 0,
                " FAIL: encoded buffer doesn't match expected\n");

    TEST_FOOTER;
    return 0;
}

static int test_decode_request_simple(void)
{
    TEST_HEADER;

    // Input: $9\r\nCREATE db\r\n
    uint8_t data[] = {'$', '9', '\r', '\n', 'C', 'R',  'E', 'A',
                      'T', 'E', ' ',  'd',  'b', '\r', '\n'};
    request_t req  = {0};

    ssize_t result = decode_request(data, &req);

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(sizeof(data), result);
    ASSERT_EQ(9, req.length);
    ASSERT_TRUE(strcmp("CREATE db", req.query) == 0,
                " FAIL: query doesn't match expecation\n");

    TEST_FOOTER;
    return 0;
}

static int test_decode_request_invalid_marker(void)
{
    TEST_HEADER;

    // Input: %9\r\nCREATE db\r\n (% instead of $)
    uint8_t data[] = {'%', '9', '\r', '\n', 'C', 'R',  'E', 'A',
                      'T', 'E', ' ',  'd',  'b', '\r', '\n'};
    request_t req  = {0};

    ssize_t result = decode_request(data, &req);

    ASSERT_EQ(-1, result);

    TEST_FOOTER;
    return 0;
}

static int test_decode_request_invalid_length(void)
{
    TEST_HEADER;

    // Input: $a\r\nCREATE db\r\n ('a' instead of digit)
    uint8_t data[] = {'$', 'a', '\r', '\n', 'C', 'R',  'E', 'A',
                      'T', 'E', ' ',  'd',  'b', '\r', '\n'};
    request_t req  = {0};

    ssize_t result = decode_request(data, &req);

    ASSERT_EQ(-1, result);

    TEST_FOOTER;
    return 0;
}

static int test_decode_request_mismatched_length(void)
{
    TEST_HEADER;

    // Input: $10\r\nCREATE db\r\n (10 instead of 9)
    uint8_t data[] = {'$', '1', '0', '\r', '\n', 'C', 'R',  'E',
                      'A', 'T', 'E', ' ',  'd',  'b', '\r', '\n'};
    request_t req  = {0};

    ssize_t result = decode_request(data, &req);

    ASSERT_EQ(-1, result);

    TEST_FOOTER;
    return 0;
}

static int test_encode_string_response(void)
{
    TEST_HEADER;

    uint8_t buffer[MAX_BUFFER_SIZE] = {0};
    char message[]                  = "OK";
    response_t resp                 = {.type            = RT_STRING,
                                       .string_response = {.rc = 0, .length = strlen(message)}};

    strncpy(resp.string_response.message, message, resp.string_response.length);

    ssize_t result     = encode_response(&resp, buffer);

    // Expected output: $2\r\nOK\r\n
    uint8_t expected[] = {'$', '2', '\r', '\n', 'O', 'K', '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);
    ASSERT_TRUE(memcmp(expected, buffer, result) == 0,
                " FAIL: encoded buffer doesn't match expected\n");

    TEST_FOOTER;
    return 0;
}

static int test_encode_error_response(void)
{
    TEST_HEADER;

    uint8_t buffer[MAX_BUFFER_SIZE] = {0};
    char message[]                  = "Error: key not found";
    response_t resp                 = {.type            = RT_STRING,
                                       .string_response = {.rc = 1, .length = strlen(message)}};

    strncpy(resp.string_response.message, message, resp.string_response.length);

    ssize_t result     = encode_response(&resp, buffer);

    // Expected output: !20\r\nError: key not found\r\n
    uint8_t expected[] = {'!', '2', '0', '\r', '\n', 'E', 'r', 'r',  'o',
                          'r', ':', ' ', 'k',  'e',  'y', ' ', 'n',  'o',
                          't', ' ', 'f', 'o',  'u',  'n', 'd', '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);
    ASSERT_TRUE(memcmp(expected, buffer, result) == 0,
                " FAIL: encoded buffer doesn't match expected\n");

    TEST_FOOTER;
    return 0;
}

static int test_encode_array_response(void)
{
    TEST_HEADER;

    uint8_t buffer[MAX_BUFFER_SIZE] = {0};

    record_t records[2]             = {{.timestamp = 1234567890, .value = 42.5},
                                       {.timestamp = 1234567891, .value = 43.7}};

    response_t resp                 = {.type           = RT_ARRAY,
                                       .array_response = {.items = records, .length = 2}};

    ssize_t result                  = encode_response(&resp, buffer);

    // Expected output format:
    // #2\r\n:1234567890\r\n;42.500000\r\n:1234567891\r\n;43.700000\r\n
    uint8_t expected[] = {'#',  '2',  '\r', '\n', ':',  '1',  '2',  '3',  '4',
                          '5',  '6',  '7',  '8',  '9',  '0',  '\r', '\n', ';',
                          '4',  '2',  '.',  '5',  '0',  '0',  '0',  '0',  '0',
                          '\r', '\n', ':',  '1',  '2',  '3',  '4',  '5',  '6',
                          '7',  '8',  '9',  '1',  '\r', '\n', ';',  '4',  '3',
                          '.',  '7',  '0',  '0',  '0',  '0',  '0',  '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_TRUE(buffer[0] == '#',
                " FAIL: array response should start with #\n");
    ASSERT_EQ(sizeof(expected), result);
    ASSERT_TRUE(memcmp(expected, buffer, sizeof(expected)) == 0,
                " FAIL: encoding doesn't match expected\n");

    TEST_FOOTER;
    return 0;
}

static int test_encode_empty_array_response(void)
{
    TEST_HEADER;

    uint8_t buffer[MAX_BUFFER_SIZE] = {0};

    response_t resp                 = {.type           = RT_ARRAY,
                                       .array_response = {.items = NULL, .length = 0}};

    ssize_t result                  = encode_response(&resp, buffer);

    // Expected output: #0\r\n
    uint8_t expected[]              = {'#', '0', '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);
    ASSERT_TRUE(memcmp(expected, buffer, result) == 0,
                " FAIL: encoded buffer doesn't match expected\n");

    TEST_FOOTER;
    return 0;
}

static int test_decode_string_response(void)
{
    TEST_HEADER;

    // Input: $2\r\nOK\r\n
    uint8_t data[]  = {'$', '2', '\r', '\n', 'O', 'K', '\r', '\n'};
    response_t resp = {0};

    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(sizeof(data), result);
    ASSERT_EQ(RT_STRING, resp.type);
    ASSERT_EQ(0, resp.string_response.rc);
    ASSERT_EQ(2, resp.string_response.length);
    ASSERT_TRUE(strcmp("OK", resp.string_response.message) == 0,
                " FAIL: message doesn't match expected\n");

    TEST_FOOTER;
    return 0;
}

static int test_decode_error_response(void)
{
    TEST_HEADER;

    // Input: !5\r\nERROR\r\n
    uint8_t data[]  = {'!', '5', '\r', '\n', 'E', 'R',
                       'R', 'O', 'R',  '\r', '\n'};
    response_t resp = {0};

    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(sizeof(data), result);
    ASSERT_EQ(RT_STRING, resp.type);
    ASSERT_EQ(1, resp.string_response.rc);
    ASSERT_EQ(5, resp.string_response.length);
    ASSERT_TRUE(strcmp("ERROR", resp.string_response.message) == 0,
                " FAIL: message doesn't match input\n");

    TEST_FOOTER;
    return 0;
}

static int test_decode_array_response(void)
{
    TEST_HEADER;

    // Input: #2\r\n:1234567890\r\n;42.5\r\n:1234567891\r\n;43.7\r\n
    uint8_t data[]  = {'#',  '2', '\r', '\n', ':',  '1',  '2',  '3',  '4',
                       '5',  '6', '7',  '8',  '9',  '0',  '\r', '\n', ';',
                       '4',  '2', '.',  '5',  '\r', '\n', ':',  '1',  '2',
                       '3',  '4', '5',  '6',  '7',  '8',  '9',  '1',  '\r',
                       '\n', ';', '4',  '3',  '.',  '7',  '\r', '\n'};

    response_t resp = {0};

    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(sizeof(data), result);
    ASSERT_EQ(RT_ARRAY, resp.type);
    ASSERT_EQ(2, resp.array_response.length);
    ASSERT_EQ(1234567890, resp.array_response.items[0].timestamp);
    ASSERT_EQ(1234567891, resp.array_response.items[1].timestamp);
    ASSERT_TRUE(resp.array_response.items[0].value == 42.5,
                " FAIL: first value doesn't match\n");
    ASSERT_TRUE(resp.array_response.items[1].value == 43.7,
                " FAIL: second value doesn't match\n");

    // Free allocated memory
    free_response(&resp);

    TEST_FOOTER;
    return 0;
}

static int test_decode_empty_array_response(void)
{
    TEST_HEADER;

    // Input: #0\r\n
    uint8_t data[]  = {'#', '0', '\r', '\n'};

    response_t resp = {0};

    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(sizeof(data), result);
    ASSERT_EQ(RT_ARRAY, resp.type);
    ASSERT_EQ(0, resp.array_response.length);
    ASSERT_TRUE(resp.array_response.items == NULL,
                " FAIL: records should be NULL\n");

    // No need to free since records is NULL

    TEST_FOOTER;
    return 0;
}

static int test_decode_array_response_invalid_format(void)
{
    TEST_HEADER;

    // Input: #2\r\n:1234567890\r\nInvalid\r\n:1234567891\r\n;43.7\r\n
    uint8_t data[]  = {'#', '2',  '\r', '\n', ':',  '1',  '2',  '3', '4',  '5',
                       '6', '7',  '8',  '9',  '0',  '\r', '\n', 'I', 'n',  'v',
                       'a', 'l',  'i',  'd',  '\r', '\n', // Missing ; marker
                       ':', '1',  '2',  '3',  '4',  '5',  '6',  '7', '8',  '9',
                       '1', '\r', '\n', ';',  '4',  '3',  '.',  '7', '\r', '\n'};

    response_t resp = {0};

    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_EQ(-1, result);

    TEST_FOOTER;
    return 0;
}

static int test_decode_stream_response_single_item(void)
{
    TEST_HEADER;

    // Input: "~1\r\n:1234567890\r\n;42.500000\r\n\r\n"
    uint8_t data[]  = {'~',  '1', '\r', '\n', ':',  '1',  '2', '3',
                       '4',  '5', '6',  '7',  '8',  '9',  '0', '\r',
                       '\n', ';', '4',  '2',  '.',  '5',  '0', '0',
                       '0',  '0', '0',  '\r', '\n', '\r', '\n'};

    response_t resp = {0};
    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(sizeof(data), result);
    ASSERT_EQ(RT_STREAM, resp.type);
    ASSERT_EQ(1, resp.stream_response.batch.length);
    ASSERT_EQ(1234567890, resp.stream_response.batch.items[0].timestamp);
    ASSERT_TRUE(resp.stream_response.batch.items[0].value == 42.5,
                " FAIL: value doesn't match\n");
    ASSERT_EQ(resp.stream_response.is_final, 0);

    // Free allocated memory
    free(resp.stream_response.batch.items);

    TEST_FOOTER;
    return 0;
}

static int test_decode_stream_response_multiple_items(void)
{
    TEST_HEADER;

    // Input:
    // "~2\r\n:1234567890\r\n;42.500000\r\n:1234567891\r\n;43.700000\r\n\r\n"
    uint8_t data[] = {'~',  '2',  '\r', '\n', ':',  '1',  '2',  '3',  '4',  '5',
                      '6',  '7',  '8',  '9',  '0',  '\r', '\n', ';',  '4',  '2',
                      '.',  '5',  '0',  '0',  '0',  '0',  '0',  '\r', '\n', ':',
                      '1',  '2',  '3',  '4',  '5',  '6',  '7',  '8',  '9',  '1',
                      '\r', '\n', ';',  '4',  '3',  '.',  '7',  '0',  '0',  '0',
                      '0',  '0',  '\r', '\n', '\r', '\n'};

    response_t resp = {0};
    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(sizeof(data), result);
    ASSERT_EQ(RT_STREAM, resp.type);
    ASSERT_EQ(2, resp.stream_response.batch.length);
    ASSERT_EQ(1234567890, resp.stream_response.batch.items[0].timestamp);
    ASSERT_EQ(1234567891, resp.stream_response.batch.items[1].timestamp);
    ASSERT_TRUE(resp.stream_response.batch.items[0].value == 42.5,
                " FAIL: first value doesn't match\n");
    ASSERT_TRUE(resp.stream_response.batch.items[1].value == 43.7,
                " FAIL: second value doesn't match\n");
    ASSERT_EQ(resp.stream_response.is_final, 0);

    // Free allocated memory
    free(resp.stream_response.batch.items);

    TEST_FOOTER;
    return 0;
}

static int test_decode_stream_response_final_chunk(void)
{
    TEST_HEADER;

    // Input: "~1\r\n:1234567890\r\n;42.500000\r\n\r\n~0\r\n"
    uint8_t data[]  = {'~',  '1',  '\r', '\n', ':', '1', '2',  '3',  '4',
                       '5',  '6',  '7',  '8',  '9', '0', '\r', '\n', ';',
                       '4',  '2',  '.',  '5',  '0', '0', '0',  '0',  '0',
                       '\r', '\n', '\r', '\n', '~', '0', '\r', '\n'};

    response_t resp = {0};
    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(result, sizeof(data));
    ASSERT_EQ(RT_STREAM, resp.type);
    ASSERT_EQ(1, resp.stream_response.batch.length);
    ASSERT_EQ(1234567890, resp.stream_response.batch.items[0].timestamp);
    ASSERT_TRUE(resp.stream_response.batch.items[0].value == 42.5,
                " FAIL: value doesn't match\n");
    ASSERT_EQ(resp.stream_response.is_final, 1);

    // Free allocated memory
    free(resp.stream_response.batch.items);

    TEST_FOOTER;
    return 0;
}

static int test_decode_stream_response_empty_batch(void)
{
    TEST_HEADER;

    // Input: "~0\r\n\r\n"
    uint8_t data[]  = {'~', '0', '\r', '\n', '\r', '\n'};

    response_t resp = {0};
    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(sizeof(data), result);
    ASSERT_EQ(RT_STREAM, resp.type);
    ASSERT_EQ(0, resp.stream_response.batch.length);
    ASSERT_EQ(NULL, resp.stream_response.batch.items);
    ASSERT_EQ(resp.stream_response.is_final, 0);

    TEST_FOOTER;
    return 0;
}

static int test_decode_stream_response_final_empty_batch(void)
{
    TEST_HEADER;

    // Input: "~0\r\n\r\n~0\r\n"
    uint8_t data[]  = {'~', '0', '\r', '\n', '\r', '\n', '~', '0', '\r', '\n'};

    response_t resp = {0};
    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(sizeof(data), result);
    ASSERT_EQ(RT_STREAM, resp.type);
    ASSERT_EQ(0, resp.stream_response.batch.length);
    ASSERT_EQ(NULL, resp.stream_response.batch.items);
    ASSERT_EQ(resp.stream_response.is_final, 1);

    TEST_FOOTER;
    return 0;
}

static int test_decode_stream_response_negative_value(void)
{
    TEST_HEADER;

    // Input: "~1\r\n:1234567890\r\n;-42.500000\r\n\r\n"
    uint8_t data[]  = {'~',  '1', '\r', '\n', ':',  '1',  '2',  '3',
                       '4',  '5', '6',  '7',  '8',  '9',  '0',  '\r',
                       '\n', ';', '-',  '4',  '2',  '.',  '5',  '0',
                       '0',  '0', '0',  '0',  '\r', '\n', '\r', '\n'};

    response_t resp = {0};
    ssize_t result  = decode_response(data, &resp, sizeof(data));

    ASSERT_TRUE(result > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(sizeof(data), result);
    ASSERT_EQ(RT_STREAM, resp.type);
    ASSERT_EQ(1, resp.stream_response.batch.length);
    ASSERT_EQ(1234567890, resp.stream_response.batch.items[0].timestamp);
    ASSERT_TRUE(resp.stream_response.batch.items[0].value == -42.5,
                " FAIL: value doesn't match\n");
    ASSERT_EQ(resp.stream_response.is_final, 0);

    // Free allocated memory
    free(resp.stream_response.batch.items);

    TEST_FOOTER;
    return 0;
}

static int test_decode_stream_response_invalid_format(void)
{
    TEST_HEADER;

    // Invalid marker
    uint8_t data1[] = {'%', '1', '\r', '\n'}; // Should start with '~'
    response_t resp = {0};
    ssize_t result  = decode_response(data1, &resp, sizeof(data1));
    ASSERT_EQ(-1, result);

    // Invalid length format
    uint8_t data2[] = {'~', 'x', '\r', '\n'}; // Non-numeric length
    result          = decode_response(data2, &resp, sizeof(data2));
    ASSERT_EQ(-1, result);

    // Missing CRLF after length
    // TODO fix this case with a proper iscrlf function accounting for
    // errors
    // uint8_t data3[] = {'~', '1', '.', '\n'};
    // result          = decode_response(data3, &resp, sizeof(data3));
    // ASSERT_EQ(-1, result);

    // Invalid timestamp marker
    uint8_t data4[] = {'~', '1', '\r', '\n', '$', '1',
                       '2', '3', '4',  '\r', '\n'}; // Should be ':'
    result          = decode_response(data4, &resp, sizeof(data4));
    ASSERT_EQ(-1, result);

    // Invalid value marker
    uint8_t data5[] = {
        '~',  '1',  '\r', '\n', ':', '1', '2', '3',  '4',
        '\r', '\n', '$',  '4',  '2', '.', '5', '\r', '\n'}; // Should be ';'
    result = decode_response(data5, &resp, sizeof(data5));
    ASSERT_EQ(-1, result);

    TEST_FOOTER;
    return 0;
}

static int test_decode_stream_response_null_parameters(void)
{
    TEST_HEADER;

    // Valid data
    uint8_t data[]  = {'~', '1', '\r', '\n', ':',  '1',  '2',  '3',  '4',
                       '5', '6', '7',  '8',  '9',  '0',  '\r', '\n', ';',
                       '4', '2', '.',  '5',  '\r', '\n', '\r', '\n'};

    // Test with NULL data
    response_t resp = {0};
    ssize_t result  = decode_response(NULL, &resp, sizeof(data));
    ASSERT_EQ(-1, result);

    // Test with NULL response
    result = decode_response(data, NULL, 0);
    ASSERT_EQ(-1, result);

    TEST_FOOTER;
    return 0;
}

static int test_free_response(void)
{
    TEST_HEADER;

    response_t resp = {.type = RT_ARRAY};

    // Set some values to ensure memory is allocated
    record_t r0     = (record_t){.timestamp = 1234567890, .value = 42.5};
    record_t r1     = (record_t){.timestamp = 1234567891, .value = 43.7};
    da_append(&resp.array_response, r0);
    da_append(&resp.array_response, r1);

    free_response(&resp);

    // Can't directly test freed memory, but we can check that the pointer
    // is NULL
    ASSERT_TRUE(resp.array_response.items == NULL,
                " FAIL: records pointer should be NULL after free\n");

    TEST_FOOTER;
    return 0;
}

static int test_request_round_trip(void)
{
    TEST_HEADER;

    // Original request
    char query[]           = "CREATE timeseries INTO db";
    request_t req_original = {.length = strlen(query)};

    strncpy(req_original.query, query, req_original.length);

    // Encode
    uint8_t buffer[MAX_BUFFER_SIZE] = {0};
    ssize_t encoded_length          = encode_request(&req_original, buffer);
    ASSERT_TRUE(encoded_length > 0, " FAIL: encoding failed\n");

    // Decode
    request_t req_decoded  = {0};

    ssize_t decoded_length = decode_request(buffer, &req_decoded);
    ASSERT_TRUE(decoded_length > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(encoded_length, decoded_length);
    ASSERT_EQ(req_original.length, req_decoded.length);
    ASSERT_TRUE(strcmp(req_original.query, req_decoded.query) == 0,
                " FAIL: original and decoded queries don't match\n");

    TEST_FOOTER;
    return 0;
}

static int test_string_response_round_trip(void)
{
    TEST_HEADER;

    // Original response
    char message[]           = "This is a test message";
    response_t resp_original = {
        .type            = RT_STRING,
        .string_response = {.rc = 0, .length = strlen(message)}};

    strncpy(resp_original.string_response.message, message, strlen(message));

    // Encode
    uint8_t buffer[MAX_BUFFER_SIZE] = {0};
    ssize_t encoded_length          = encode_response(&resp_original, buffer);
    ASSERT_TRUE(encoded_length > 0, " FAIL: Encoding failed\n");

    // Decode
    response_t resp_decoded = {0};

    ssize_t decoded_length =
        decode_response(buffer, &resp_decoded, encoded_length);
    ASSERT_TRUE(decoded_length > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(encoded_length, decoded_length);
    ASSERT_EQ(resp_original.type, resp_decoded.type);
    ASSERT_EQ(resp_original.string_response.rc,
              resp_decoded.string_response.rc);
    ASSERT_EQ(resp_original.string_response.length,
              resp_decoded.string_response.length);
    ASSERT_TRUE(strcmp(resp_original.string_response.message,
                       resp_decoded.string_response.message) == 0,
                " FAIL: original and decoded messages don't match\n");

    TEST_FOOTER;
    return 0;
}

static int test_array_response_round_trip(void)
{
    TEST_HEADER;

    // Create a small array
    record_t records[3]      = {{.timestamp = 1000000001, .value = 10.1},
                                {.timestamp = 1000000002, .value = 20.2},
                                {.timestamp = 1000000003, .value = 30.3}};

    // Original response
    response_t resp_original = {
        .type = RT_ARRAY, .array_response = {.items = records, .length = 3}};

    // Encode
    uint8_t buffer[MAX_BUFFER_SIZE] = {0};
    ssize_t encoded_length          = encode_response(&resp_original, buffer);
    ASSERT_TRUE(encoded_length > 0, "Encoding should succeed");

    // Decode
    response_t resp_decoded = {0};

    ssize_t decoded_length =
        decode_response(buffer, &resp_decoded, encoded_length);
    ASSERT_TRUE(decoded_length > 0, " FAIL: decoding failed\n");
    ASSERT_EQ(encoded_length, decoded_length);
    ASSERT_EQ(resp_original.type, resp_decoded.type);
    ASSERT_EQ(resp_original.array_response.length,
              resp_decoded.array_response.length);

    // Compare records
    for (size_t i = 0; i < resp_original.array_response.length; i++) {
        ASSERT_EQ(resp_original.array_response.items[i].timestamp,
                  resp_decoded.array_response.items[i].timestamp);

        ASSERT_TRUE(
            fequals(resp_original.array_response.items[i].value,
                    resp_decoded.array_response.items[i].value),
            " FAIL: original and decoded values don't be approximately equal");
    }

    // Clean up
    free_response(&resp_decoded);

    TEST_FOOTER;
    return 0;
}

static int test_encode_stream_response_single_item(void)
{
    TEST_HEADER;

    // Create a response with a single item
    response_t resp                               = {0};
    resp.type                                     = RT_STREAM;
    resp.stream_response.batch.length             = 1;
    resp.stream_response.batch.items              = malloc(sizeof(record_t));
    resp.stream_response.batch.items[0].timestamp = 1234567890;
    resp.stream_response.batch.items[0].value     = 42.5;
    resp.stream_response.is_final                 = false;

    // Buffer to hold the encoded data
    uint8_t buffer[QUERYSIZE]                     = {0};

    // Encode the response
    ssize_t result     = encode_response(&resp, buffer);

    // Expected output: "~1\r\n:1234567890\r\n;42.500000\r\n\r\n"
    uint8_t expected[] = {'~',  '1', '\r', '\n', ':',  '1',  '2', '3',
                          '4',  '5', '6',  '7',  '8',  '9',  '0', '\r',
                          '\n', ';', '4',  '2',  '.',  '5',  '0', '0',
                          '0',  '0', '0',  '\r', '\n', '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);

    for (size_t i = 0; i < sizeof(expected); i++) {
        ASSERT_EQ(expected[i], buffer[i]);
    }

    // Free allocated memory
    free(resp.stream_response.batch.items);

    TEST_FOOTER;
    return 0;
}

static int test_encode_stream_response_multiple_items(void)
{
    TEST_HEADER;

    // Create a response with multiple items
    response_t resp                   = {0};
    resp.type                         = RT_STREAM;
    resp.stream_response.batch.length = 2;
    resp.stream_response.batch.items  = malloc(2 * sizeof(record_t));
    resp.stream_response.batch.items[0].timestamp = 1234567890;
    resp.stream_response.batch.items[0].value     = 42.5;
    resp.stream_response.batch.items[1].timestamp = 1234567891;
    resp.stream_response.batch.items[1].value     = 43.7;
    resp.stream_response.is_final                 = false;

    // Buffer to hold the encoded data
    uint8_t buffer[QUERYSIZE]                     = {0};

    // Encode the response
    ssize_t result     = encode_response(&resp, buffer);

    // Expected output:
    // "~2\r\n:1234567890\r\n;42.500000\r\n:1234567891\r\n;43.700000\r\n\r\n"
    uint8_t expected[] = {
        '~', '2', '\r', '\n', ':',  '1',  '2',  '3', '4', '5', '6', '7',
        '8', '9', '0',  '\r', '\n', ';',  '4',  '2', '.', '5', '0', '0',
        '0', '0', '0',  '\r', '\n', ':',  '1',  '2', '3', '4', '5', '6',
        '7', '8', '9',  '1',  '\r', '\n', ';',  '4', '3', '.', '7', '0',
        '0', '0', '0',  '0',  '\r', '\n', '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);

    for (size_t i = 0; i < sizeof(expected); i++) {
        ASSERT_EQ(expected[i], buffer[i]);
    }

    // Free allocated memory
    free(resp.stream_response.batch.items);

    TEST_FOOTER;
    return 0;
}

static int test_encode_stream_response_final_chunk(void)
{
    TEST_HEADER;

    // Create a response with a final chunk flag
    response_t resp                               = {0};
    resp.type                                     = RT_STREAM;
    resp.stream_response.batch.length             = 1;
    resp.stream_response.batch.items              = malloc(sizeof(record_t));
    resp.stream_response.batch.items[0].timestamp = 1234567890;
    resp.stream_response.batch.items[0].value     = 42.5;
    resp.stream_response.is_final                 = true;

    // Buffer to hold the encoded data
    uint8_t buffer[QUERYSIZE]                     = {0};

    // Encode the response
    ssize_t result     = encode_response(&resp, buffer);

    // Expected output: "~1\r\n:1234567890\r\n;42.500000\r\n\r\n~0\r\n"
    uint8_t expected[] = {'~',  '1',  '\r', '\n', ':', '1', '2',  '3',  '4',
                          '5',  '6',  '7',  '8',  '9', '0', '\r', '\n', ';',
                          '4',  '2',  '.',  '5',  '0', '0', '0',  '0',  '0',
                          '\r', '\n', '\r', '\n', '~', '0', '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);

    for (size_t i = 0; i < sizeof(expected); i++) {
        ASSERT_EQ(expected[i], buffer[i]);
    }

    // Free allocated memory
    free(resp.stream_response.batch.items);

    TEST_FOOTER;
    return 0;
}

static int test_encode_stream_response_empty_batch(void)
{
    TEST_HEADER;

    // Create a response with an empty batch
    response_t resp                   = {0};
    resp.type                         = RT_STREAM;
    resp.stream_response.batch.length = 0;
    resp.stream_response.batch.items  = NULL;
    resp.stream_response.is_final     = false;

    // Buffer to hold the encoded data
    uint8_t buffer[QUERYSIZE]         = {0};

    // Encode the response
    ssize_t result                    = encode_response(&resp, buffer);

    // Expected output: "~0\r\n\r\n"
    uint8_t expected[]                = {'~', '0', '\r', '\n', '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);

    for (size_t i = 0; i < sizeof(expected); i++) {
        ASSERT_EQ(expected[i], buffer[i]);
    }

    TEST_FOOTER;
    return 0;
}

static int test_encode_stream_response_negative_value(void)
{
    TEST_HEADER;

    // Create a response with a negative value
    response_t resp                               = {0};
    resp.type                                     = RT_STREAM;
    resp.stream_response.batch.length             = 1;
    resp.stream_response.batch.items              = malloc(sizeof(record_t));
    resp.stream_response.batch.items[0].timestamp = 1234567890;
    resp.stream_response.batch.items[0].value     = -42.5;
    resp.stream_response.is_final                 = false;

    // Buffer to hold the encoded data
    uint8_t buffer[QUERYSIZE]                     = {0};

    // Encode the response
    ssize_t result     = encode_response(&resp, buffer);

    // Expected output: "~1\r\n:1234567890\r\n;-42.500000\r\n\r\n"
    uint8_t expected[] = {'~',  '1', '\r', '\n', ':',  '1',  '2',  '3',
                          '4',  '5', '6',  '7',  '8',  '9',  '0',  '\r',
                          '\n', ';', '-',  '4',  '2',  '.',  '5',  '0',
                          '0',  '0', '0',  '0',  '\r', '\n', '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);

    for (size_t i = 0; i < sizeof(expected); i++) {
        ASSERT_EQ(expected[i], buffer[i]);
    }

    // Free allocated memory
    free(resp.stream_response.batch.items);

    TEST_FOOTER;
    return 0;
}

static int test_encode_stream_response_buffer_overflow(void)
{
    TEST_HEADER;

    // Create a response with many items to test buffer overflow
    const size_t HUGE_LENGTH =
        QUERYSIZE; // This should be large enough to overflow

    response_t resp                   = {0};
    resp.type                         = RT_STREAM;
    resp.stream_response.batch.length = HUGE_LENGTH;
    resp.stream_response.batch.items  = malloc(HUGE_LENGTH * sizeof(record_t));

    for (size_t i = 0; i < HUGE_LENGTH; i++) {
        resp.stream_response.batch.items[i].timestamp = 1234567890 + i;
        resp.stream_response.batch.items[i].value     = 42.5 + i;
    }

    resp.stream_response.is_final = false;

    // Buffer to hold the encoded data
    uint8_t buffer[QUERYSIZE]     = {0};

    // Encode the response - should fail due to buffer overflow
    ssize_t result                = encode_response(&resp, buffer);

    ASSERT_EQ(-1, result); // Expect failure

    // Free allocated memory
    free(resp.stream_response.batch.items);

    TEST_FOOTER;
    return 0;
}

static int test_encode_stream_response_null_parameters(void)
{
    TEST_HEADER;

    // Test with NULL response
    uint8_t buffer[QUERYSIZE] = {0};
    ssize_t result            = encode_response(NULL, buffer);
    ASSERT_EQ(-1, result);

    // Create a valid response
    response_t resp                               = {0};
    resp.type                                     = RT_STREAM;
    resp.stream_response.batch.length             = 1;
    resp.stream_response.batch.items              = malloc(sizeof(record_t));
    resp.stream_response.batch.items[0].timestamp = 1234567890;
    resp.stream_response.batch.items[0].value     = 42.5;
    resp.stream_response.is_final                 = false;

    // Test with NULL buffer
    result = encode_response(&resp, NULL);
    ASSERT_EQ(-1, result);

    // Free allocated memory
    free(resp.stream_response.batch.items);

    TEST_FOOTER;
    return 0;
}

static int test_encode_stream_response_final_empty_batch(void)
{
    TEST_HEADER;

    // Create a final empty batch response
    response_t resp                   = {0};
    resp.type                         = RT_STREAM;
    resp.stream_response.batch.length = 0;
    resp.stream_response.batch.items  = NULL;
    resp.stream_response.is_final     = true;

    // Buffer to hold the encoded data
    uint8_t buffer[QUERYSIZE]         = {0};

    // Encode the response
    ssize_t result                    = encode_response(&resp, buffer);

    // Expected output: "~0\r\n\r\n~0\r\n"
    uint8_t expected[]                = {'~',  '0', '\r', '\n', '\r',
                                         '\n', '~', '0',  '\r', '\n'};

    ASSERT_TRUE(result > 0, " FAIL: encoding failed\n");
    ASSERT_EQ(sizeof(expected), result);

    for (size_t i = 0; i < sizeof(expected); i++) {
        ASSERT_EQ(expected[i], buffer[i]);
    }

    TEST_FOOTER;
    return 0;
}

int encoding_test(void)
{
    printf("* %s\n\n", __FUNCTION__);

    int cases   = 35;
    int success = cases;

    // Request encoding tests
    success += test_encode_request_simple();
    success += test_encode_request_empty();

    // Request decoding tests
    success += test_decode_request_simple();
    success += test_decode_request_invalid_marker();
    success += test_decode_request_invalid_length();
    success += test_decode_request_mismatched_length();

    // Response encoding tests
    success += test_encode_string_response();
    success += test_encode_error_response();
    success += test_encode_array_response();
    success += test_encode_empty_array_response();
    success += test_encode_stream_response_single_item();
    success += test_encode_stream_response_multiple_items();
    success += test_encode_stream_response_final_chunk();
    success += test_encode_stream_response_empty_batch();
    success += test_encode_stream_response_negative_value();
    success += test_encode_stream_response_buffer_overflow();
    success += test_encode_stream_response_null_parameters();
    success += test_encode_stream_response_final_empty_batch();

    // Response decoding tests
    success += test_decode_string_response();
    success += test_decode_error_response();
    success += test_decode_array_response();
    success += test_decode_empty_array_response();
    success += test_decode_array_response_invalid_format();
    success += test_decode_stream_response_single_item();
    success += test_decode_stream_response_multiple_items();
    success += test_decode_stream_response_final_chunk();
    success += test_decode_stream_response_empty_batch();
    success += test_decode_stream_response_final_empty_batch();
    success += test_decode_stream_response_negative_value();
    success += test_decode_stream_response_invalid_format();
    success += test_decode_stream_response_null_parameters();

    // Memory management test
    success += test_free_response();

    // Round-trip tests
    success += test_request_round_trip();
    success += test_string_response_round_trip();
    success += test_array_response_round_trip();

    printf("\n Test suite summary: %d passed, %d failed\n", success,
           cases - success);

    return 0;
}
