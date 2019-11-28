#include <iostream>
#include "com_odps_arrow_TestArrowJni.h"

#include "arrow/array.h"
#include "arrow/io/test_common.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/io/test_common.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/test_common.h"
#include "arrow/ipc/writer.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"

using namespace std;
using namespace arrow;


#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_niki_test_jni_TestArrow
 * Method:    createVector
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT void JNICALL Java_com_odps_arrow_TestArrowJni_javaToNative
(JNIEnv *env, jclass cls, jbyteArray byteArray, jlongArray buf_addrs, jlongArray buf_sizes, jint value_count){

    cout << "java to native!" << endl;

    jsize schema_size = env->GetArrayLength(byteArray);
    jbyte* schema_bytes = env->GetByteArrayElements(byteArray, 0);
    jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
    jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

    // read schema
    shared_ptr<arrow::Buffer> buffer;
    arrow::AllocateBuffer(schema_size, &buffer);

    arrow::BufferBuilder builder;
    builder.Append(reinterpret_cast<uint8_t*>(schema_bytes), schema_size);
    builder.Finish(&buffer);

    arrow::io::BufferReader reader(buffer);
    std::shared_ptr<arrow::Schema> read_schema;
    arrow::ipc::ReadSchema(&reader, nullptr, &read_schema);

    cout << "read schema:\n" << read_schema->ToString() << endl;


    // generate record batch
    vector<shared_ptr<ArrayData>> columns;
    auto num_fields = read_schema->num_fields();
    int buf_idx = 0;
    int size_idx = 0;

    for (int i = 0; i < num_fields; i++) {
    auto field = read_schema->field(i);
    vector<shared_ptr<Buffer>> buffers;

    // create validity buffer
    jlong validity_addr = in_buf_addrs[buf_idx++];
    jlong validity_size = in_buf_sizes[size_idx++];
    auto validity = shared_ptr<Buffer>(
            new Buffer(reinterpret_cast<uint8_t*>(validity_addr), validity_size));
    buffers.push_back(validity);

    // create data buffer
    jlong value_addr = in_buf_addrs[buf_idx++];
    jlong value_size = in_buf_sizes[size_idx++];
    auto data = shared_ptr<Buffer>(
            new Buffer(reinterpret_cast<uint8_t*>(value_addr), value_size));
    buffers.push_back(data);

    if (arrow::is_binary_like(field->type()->id())) {

    // add offsets buffer for variable-len fields.
    jlong offsets_addr = in_buf_addrs[buf_idx++];
    jlong offsets_size = in_buf_sizes[size_idx++];
    auto offsets = shared_ptr<Buffer>(
            new Buffer(reinterpret_cast<uint8_t*>(offsets_addr), offsets_size));
    buffers.push_back(offsets);
    }

    auto array_data = arrow::ArrayData::Make(field->type(), value_count, std::move(buffers));
    columns.push_back(array_data);
    }
    shared_ptr<RecordBatch> batch;
    batch = RecordBatch::Make(read_schema, value_count, columns);

    cout << "vectors from java:" << endl;
    for (int i = 0; i < batch->num_columns(); i++) {
        cout << batch->column(i)->ToString() << endl;
    }

}


/*
 * Class:     com_niki_test_jni_TestArrow
 * Method:    nativeToJava
 * Signature: ()Lcom/niki/test/jni/DataNode;
 */
JNIEXPORT jobject JNICALL Java_com_odps_arrow_TestArrowJni_nativeToJava
        (JNIEnv *env, jclass cls) {

    vector<shared_ptr<Array>> arrays;

    Int32Builder builder;
    builder.Append(1);
    builder.Append(2);
    builder.Append(3);
    std::shared_ptr<arrow::Array> int_array;
    builder.Finish(&int_array);
    arrays.push_back(int_array);

    auto f0 = field("f0", int_array->type());
    auto schema = arrow::schema({f0});
    auto batch = RecordBatch::Make(schema, 3, arrays);

    // serialize schema
    shared_ptr<Buffer> out;
    arrow::ipc::SerializeSchema(*schema, nullptr, arrow::default_memory_pool(), &out);

    jbyteArray ret = env->NewByteArray(out->size());
    auto src = reinterpret_cast<const jbyte*>(out->data());
    env->SetByteArrayRegion(ret, 0, out->size(), src);

    std::cout << "create node!" << std::endl;

    jlongArray buf_addrs = env->NewLongArray(2);
    jlongArray buf_sizes = env->NewLongArray(2);
    jlong *out_buf_addrs = env->GetLongArrayElements(buf_addrs, NULL);
    jlong *out_buf_sizes = env->GetLongArrayElements(buf_sizes, NULL);


    int idx = 0;

    for (int i = 0; i < batch->num_columns(); i++) {
        shared_ptr<arrow::Array> array = batch->column(i);
        vector<shared_ptr<Buffer>> buffers = array->data()->buffers;

        for (int k = 0; k < buffers.size(); k++) {
            shared_ptr<Buffer> buffer = buffers.at(k);
            out_buf_addrs[idx] = (jlong) buffer->data();
            out_buf_sizes[idx] = (jlong) buffer->size();
            idx++;
        }
    }

    env->SetLongArrayRegion(buf_addrs, 0, idx, out_buf_addrs);
    env->SetLongArrayRegion(buf_sizes, 0, idx, out_buf_sizes);

    jclass node_class = env->FindClass("Lcom/odps/arrow/DataNode;");
    jmethodID node_constructor = env->GetMethodID(node_class, "<init>", "([B[J[JI)V");
    jobject node = env->NewObject(node_class, node_constructor, ret, buf_addrs, buf_sizes, int_array->length());
    return node;
}

#ifdef __cplusplus
}
#endif