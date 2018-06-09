#include <Columns/ColumnWithDictionary.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <Core/TypeListNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeWithDictionary.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    const ColumnWithDictionary & getColumnWithDictionary(const IColumn & column)
    {
        return typeid_cast<const ColumnWithDictionary &>(column);
    }

    ColumnWithDictionary & getColumnWithDictionary(IColumn & column)
    {
        return typeid_cast<ColumnWithDictionary &>(column);
    }
}

DataTypeWithDictionary::DataTypeWithDictionary(DataTypePtr dictionary_type_, DataTypePtr indexes_type_)
        : dictionary_type(std::move(dictionary_type_)), indexes_type(std::move(indexes_type_))
{
    if (!indexes_type->isUnsignedInteger())
        throw Exception("Index type of DataTypeWithDictionary must be unsigned integer, but got "
                        + indexes_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    auto inner_type = dictionary_type;
    if (dictionary_type->isNullable())
        inner_type = static_cast<const DataTypeNullable &>(*dictionary_type).getNestedType();

    if (!inner_type->isStringOrFixedString()
        && !inner_type->isDateOrDateTime()
        && !inner_type->isNumber())
        throw Exception("DataTypeWithDictionary is supported only for numbers, strings, Date or DateTime, but got "
                        + dictionary_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

void DataTypeWithDictionary::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    path.push_back(Substream::DictionaryKeys);
    dictionary_type->enumerateStreams(callback, path);
    path.back() = Substream::DictionaryIndexes;
    indexes_type->enumerateStreams(callback, path);
    path.pop_back();
}

struct  KeysSerializationVersion
{
    /// Write keys as full column. No indexes is written. Structure:
    ///   <name>.dict.bin : [version - 32 bits][keys]
    ///   <name>.dict.mrk : [marks for keys]
    // FullColumn = 0,
    /// Write all keys in serializePostfix and read in deserializePrefix.
    ///   <name>.dict.bin : [version - 32 bits][indexes type - 32 bits][keys]
    ///   <name>.bin : [indexes]
    ///   <name>.mrk : [marks for indexes]
    // SingleDictionary,
    /// Write distinct set of keys for each granule. Structure:
    ///   <name>.dict.bin : [version - 32 bits][indexes type - 32 bits][keys]
    ///   <name>.dict.mrk : [marks for keys]
    ///   <name>.bin : [indexes]
    ///   <name>.mrk : [marks for indexes]
    // DictionaryPerGranule,

    enum Value
    {
        SingleDictionaryWithAdditionalKeysPerBlock = 1,
    };

    Value value;

    static void checkVersion(UInt64 version)
    {
        if (version != SingleDictionaryWithAdditionalKeysPerBlock)
            throw Exception("Invalid version for DataTypeWithDictionary key column.", ErrorCodes::LOGICAL_ERROR);
    }

    KeysSerializationVersion(UInt64 version) : value(static_cast<Value>(version)) { checkVersion(version); }
};

enum class IndexesSerializationType
{
    UInt8 = 0,
    UInt16,
    UInt32,
    UInt64,

    UInt8WithAdditionalKeys = 256,
    UInt16WithAdditionalKeys,
    UInt32WithAdditionalKeys,
    UInt64WithAdditionalKeys,
};

IndexesSerializationType dataTypeToIndexesSerializationType(const IDataType & type, bool with_additional_keys)
{
    if (with_additional_keys)
    {
        if (typeid_cast<const DataTypeUInt8 *>(&type))
            return IndexesSerializationType::UInt8WithAdditionalKeys;
        if (typeid_cast<const DataTypeUInt16 *>(&type))
            return IndexesSerializationType::UInt16WithAdditionalKeys;
        if (typeid_cast<const DataTypeUInt32 *>(&type))
            return IndexesSerializationType::UInt32WithAdditionalKeys;
        if (typeid_cast<const DataTypeUInt64 *>(&type))
            return IndexesSerializationType::UInt64WithAdditionalKeys;
    }
    else
    {
        if (typeid_cast<const DataTypeUInt8 *>(&type))
            return IndexesSerializationType::UInt8;
        if (typeid_cast<const DataTypeUInt16 *>(&type))
            return IndexesSerializationType::UInt16;
        if (typeid_cast<const DataTypeUInt32 *>(&type))
            return IndexesSerializationType::UInt32;
        if (typeid_cast<const DataTypeUInt64 *>(&type))
            return IndexesSerializationType::UInt64;
    }

    throw Exception("Invalid DataType for IndexesSerializationType. Expected UInt*, got " + type.getName(),
                    ErrorCodes::LOGICAL_ERROR);
}

struct SerializeStateWithDictionary : public IDataType::SerializeBinaryBulkState
{
    KeysSerializationVersion key_version;
    IColumnUnique::MutablePtr column_unique;

    explicit SerializeStateWithDictionary(
        UInt64 key_version,
        IColumnUnique::MutablePtr && column_unique)
        : key_version(key_version)
        , column_unique(std::move(column_unique)) {}
};

struct DeserializeStateWithDictionary : public IDataType::DeserializeBinaryBulkState
{
    KeysSerializationVersion key_version;
    IColumnUnique::MutablePtr column_unique;

    explicit DeserializeStateWithDictionary(
        UInt64 key_version,
        IColumnUnique::MutablePtr && column_unique)
        : key_version(key_version)
        , column_unique(std::move(column_unique)) {}
};

static SerializeStateWithDictionary * checkAndGetWithDictionarySerializeState(
    IDataType::SerializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeWithDictionary.", ErrorCodes::LOGICAL_ERROR);

    auto * with_dictionary_state = typeid_cast<SerializeStateWithDictionary *>(state.get());
    if (!with_dictionary_state)
        throw Exception("Invalid SerializeBinaryBulkState for DataTypeWithDictionary. Expected: "
                        + demangle(typeid(SerializeStateWithDictionary).name()) + ", got "
                        + demangle(typeid(*state).name()), ErrorCodes::LOGICAL_ERROR);

    return with_dictionary_state;
}

static DeserializeStateWithDictionary * checkAndGetWithDictionaryDeserializeState(
    IDataType::DeserializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeWithDictionary.", ErrorCodes::LOGICAL_ERROR);

    auto * with_dictionary_state = typeid_cast<DeserializeStateWithDictionary *>(state.get());
    if (!with_dictionary_state)
        throw Exception("Invalid DeserializeBinaryBulkState for DataTypeWithDictionary. Expected: "
                        + demangle(typeid(DeserializeStateWithDictionary).name()) + ", got "
                        + demangle(typeid(*state).name()), ErrorCodes::LOGICAL_ERROR);

    return with_dictionary_state;
}

//
//void DataTypeWithDictionary::serializeBinaryBulkStatePrefix(OutputStreamGetter getter, SubstreamPath path) const
//{
//    path.push_back(Substream::DictionaryKeys);
//    auto * stream = getter(path);
//
//    if (!stream)
//        throw Exception("Got empty stream in DataTypeWithDictionary::serializeBinaryBulkStatePrefix",
//                        ErrorCodes::LOGICAL_ERROR);
//
//    /// TODO: select serialization method here.
//    KeysSerializationVersion keys_ser_version = KeysSerializationVersion::DictionaryPerGranule;
//    IndexesSerializationType indexes_ser_type = dataTypeToIndexesSerializationType(*indexes_type);
//
//    auto keys_ser_version_val = static_cast<UInt32>(keys_ser_version);
//    auto indexes_ser_type_val = static_cast<UInt32>(indexes_ser_type);
//
//    writeIntBinary(keys_ser_version_val, *stream);
//    writeIntBinary(indexes_ser_type_val, *stream);
//
//    path.push_back(IDataType::Substream::DictionaryKeys);
//    auto keys_state = dictionary_type->serializeBinaryBulkStatePrefix(getter, path);
//    return std::make_shared<SerializeStateWithDictionaryPerGranule>(
//            std::move(keys_state), keys_ser_version, indexes_ser_type);
//}


void DataTypeWithDictionary::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * state_with_dictionary = checkAndGetWithDictionarySerializeState(state);
    KeysSerializationVersion::checkVersion(state_with_dictionary->key_version.value);

    if (state_with_dictionary->column_unique)
    {
        settings.path.push_back(Substream::DictionaryKeys);
        auto * stream = settings.getter(settings.path);
        settings.path.pop_back();

        if (!stream)
            throw Exception("Got empty stream in DataTypeWithDictionary::serializeBinaryBulkStateSuffix",
                            ErrorCodes::LOGICAL_ERROR);

        auto unique_state = state_with_dictionary->column_unique->getSerializableState();
        dictionary_type->serializeBinaryBulk(*unique_state.column, *stream, unique_state.offset, unique_state.limit);
    }
}

void DataTypeWithDictionary::deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::DictionaryKeys);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception("Got empty stream in DataTypeWithDictionary::deserializeBinaryBulkStatePrefix",
                        ErrorCodes::LOGICAL_ERROR);

    UInt64 keys_version;
    readIntBinary(keys_version, *stream);

    auto column_unique = static_cast<ColumnWithDictionary &>(*createColumn()).assumeMutable();
    state = std::make_shared<DeserializeStateWithDictionary>(keys_version, std::move(column_unique));
}

void DataTypeWithDictionary::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::DictionaryKeys);
    auto * keys_stream = settings.getter(settings.path);
    settings.path.back() = Substream::DictionaryIndexes;
    auto * indexes_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!keys_stream && !indexes_stream)
        return;

    if (!keys_stream)
        throw Exception("Got empty stream for DataTypeWithDictionary keys.", ErrorCodes::LOGICAL_ERROR);

    if (!indexes_stream)
        throw Exception("Got empty stream for DataTypeWithDictionary indexes.", ErrorCodes::LOGICAL_ERROR);

    const ColumnWithDictionary & column_with_dictionary = typeid_cast<const ColumnWithDictionary &>(column);

    if (!state)
    {
        /// Write version and create SerializeBinaryBulkState.
        UInt64 key_version = KeysSerializationVersion::SingleDictionaryWithAdditionalKeysPerBlock;

        writeIntBinary(key_version, *keys_stream);

        auto column_unique = static_cast<ColumnWithDictionary &>(*createColumn()).assumeMutable();
        state = std::make_shared<SerializeStateWithDictionary>(key_version, std::move(column_unique));
    }

    auto * state_with_dictionary = checkAndGetWithDictionarySerializeState(state);
    KeysSerializationVersion::checkVersion(state_with_dictionary->key_version.value);

    auto unique_state = state_with_dictionary->column_unique->getSerializableState();
    bool was_global_dictionary_written = unique_state.limit >= settings.max_dictionary_size;

    const auto & indexes = column_with_dictionary.getIndexesPtr();
    const auto & keys = column_with_dictionary.getUnique()->getNestedColumn();

    size_t max_limit = column.size() - offset;
    limit = limit ? std::min(limit, max_limit) : max_limit;

    /// Create pair (used_keys, sub_index) which is the dictionary for [offset, offset + limit) range.
    MutableColumnPtr sub_index = (*indexes->cut(offset, limit)).mutate();
    ColumnPtr unique_indexes = makeSubIndex(*sub_index);
    /// unique_indexes->index(sub_index) == indexes[offset:offset + limit]
    MutableColumnPtr used_keys = (*keys->index(unique_indexes, 0)).mutate();

    if (settings.max_dictionary_size)
    {
        /// Insert used_keys into global dictionary and update sub_index.
        auto global_indexes = state_with_dictionary->column_unique->uniqueInsertRangeFrom(
                *used_keys, 0, used_keys->size(), settings.max_dictionary_size);

        /// TODO: filter indexes > settings.max_dictionary_size

        sub_index = (*global_indexes->index(std::move(sub_index), 0)).mutate();

        /// TODO
    }

    /// Check is we need to write additional keys.
    unique_state = state_with_dictionary->column_unique->getSerializableState();
    bool need_additional_keys = unique_state.limit > settings.max_dictionary_size;

    auto index_version = dataTypeToIndexesSerializationType(*indexes_type, need_additional_keys);
    auto index_version_value = static_cast<UInt64>(index_version);
    writeIntBinary(index_version, *indexes_stream);

    if (!was_global_dictionary_written && unique_state.limit >= settings.max_dictionary_size)
    {
        /// Write global dictionary if it wasn't written and has too many keys.
        UInt64 num_keys = settings.max_dictionary_size;
        writeIntBinary(num_keys, *keys_stream);
        dictionary_type->serializeBinaryBulk(*unique_state.column, *keys_stream, unique_state.offset, num_keys);
    }

    if (need_additional_keys)
    {
        UInt64 num_keys = unique_state.limit - settings.max_dictionary_size;
        writeIntBinary(num_keys, *keys_stream);
        auto additional_offset = unique_state.offset + settings.max_dictionary_size;
        dictionary_type->serializeBinaryBulk(*unique_state.column, *keys_stream, additional_offset, num_keys);
    }

    indexes_type->serializeBinaryBulk(*sub_index, *indexes_stream, 0, 0);
}

void DataTypeWithDictionary::deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        InputStreamGetter getter,
        size_t limit,
        double /*avg_value_size_hint*/,
        bool position_independent_encoding,
        SubstreamPath path,
        const DeserializeBinaryBulkStatePtr & state) const
{
    ColumnWithDictionary & column_with_dictionary = typeid_cast<ColumnWithDictionary &>(column);

    auto * dict_state = dynamic_cast<DeserializeBinaryBulkStateWithDictionary *>(state.get());
    if (!dict_state)
        throw Exception("Invalid DeserializeBinaryBulkState.", ErrorCodes::LOGICAL_ERROR);

    if (dict_state->key_version != KeysSerializationVersion::DictionaryPerGranule)
        throw Exception("Unsupported KeysSerializationVersion for DataTypeWithDictionary", ErrorCodes::LOGICAL_ERROR);

    auto * dict_state_per_granule = typeid_cast<DeserializeStateWithDictionaryPerGranule *>(dict_state);

    auto readIndexes = [&](ReadBuffer * stream, const ColumnPtr & index, size_t num_rows)
    {
        auto index_col = indexes_type->createColumn();
        indexes_type->deserializeBinaryBulk(*index_col, *stream, num_rows, 0);
        column_with_dictionary.getIndexes()->insertRangeFrom(*index->index(std::move(index_col), 0), 0, num_rows);
    };

    using CachedStreams = std::unordered_map<std::string, ReadBuffer *>;
    CachedStreams cached_streams;

    IDataType::InputStreamGetter cached_stream_getter = [&] (const IDataType::SubstreamPath & path) -> ReadBuffer *
    {
        std::string stream_name = IDataType::getFileNameForStream("", path);
        auto iter = cached_streams.find(stream_name);
        if (iter == cached_streams.end())
            iter = cached_streams.insert({stream_name, getter(path)}).first;
        return iter->second;
    };

    auto readDict = [&](UInt64 num_keys)
    {
        auto dict_column = dictionary_type->createColumn();
        dictionary_type->deserializeBinaryBulkWithMultipleStreams(
                *dict_column, cached_stream_getter, num_keys, 0,
                position_independent_encoding, path, dict_state->keys_state);
        return column_with_dictionary.getUnique()->uniqueInsertRangeFrom(*dict_column, 0, num_keys);
    };

    path.push_back(Substream::DictionaryKeys);
    if (auto stream = cached_stream_getter(path))
    {
        UInt64 num_keys;
        readIntBinary(num_keys, *stream);

        auto dict_column = dictionary_type->createColumn();
        dictionary_type->deserializeBinaryBulkWithMultipleStreams(
                *dict_column, cached_stream_getter, num_keys, 0,
                position_independent_encoding, path, dict_state->keys_state);

        dict_state_per_granule->column_unique = std::move(dict_column);
    }


    path.push_back(Substream::DictionaryIndexes);

    if (auto stream = getter(path))
    {
        path.back() = Substream::DictionaryKeys;

        while (limit)
        {
            if (dict_state_per_granule->num_rows_to_read_until_next_index == 0)
            {
                if (stream->eof())
                    break;

                UInt64 num_keys;
                readIntBinary(num_keys, *stream);
                readIntBinary(dict_state_per_granule->num_rows_to_read_until_next_index, *stream);
                dict_state_per_granule->column_unique = readDict(num_keys);
            }

            size_t num_rows_to_read = std::min(limit, dict_state_per_granule->num_rows_to_read_until_next_index);
            readIndexes(stream, dict_state_per_granule->column_unique, num_rows_to_read);
            limit -= num_rows_to_read;
            dict_state_per_granule->num_rows_to_read_until_next_index -= num_rows_to_read;
        }
    }
}

void DataTypeWithDictionary::serializeBinaryBulkFromSingleColumn(
        const IColumn & column,
        WriteBuffer & ostr,
        size_t offset,
        size_t limit,
        bool position_independent_encoding) const
{
    const auto & column_with_dictionary = static_cast<const ColumnWithDictionary &>(column);

    size_t max_limit = column.size() - offset;
    limit = limit ? std::min(limit, max_limit) : max_limit;

    const auto & indexes = column_with_dictionary.getIndexesPtr();
    const auto & keys = column_with_dictionary.getUnique()->getNestedColumn();
    MutableColumnPtr sub_index = (*indexes->cut(offset, limit)).mutate();
    ColumnPtr unique_indexes = makeSubIndex(*sub_index);
    /// unique_indexes->index(sub_index) == indexes[offset:offset + limit]
    auto used_keys = keys->index(unique_indexes, 0);
    /// (used_keys, sub_index) is ColumnWithDictionary for range [offset:offset + limit]

    UInt64 used_keys_size = used_keys->size();
    writeIntBinary(used_keys_size, ostr);

    auto indexes_ser_type = static_cast<UInt32>(dataTypeToIndexesSerializationType(*indexes_type));
    writeIntBinary(indexes_ser_type, ostr);

    dictionary_type->serializeBinaryBulkFromSingleColumn(*used_keys, ostr, 0, 0, position_independent_encoding);
    indexes_type->serializeBinaryBulk(*sub_index, ostr, 0, limit);
}

void DataTypeWithDictionary::deserializeBinaryBulkToSingleColumn(
        IColumn & column,
        ReadBuffer & istr,
        size_t limit,
        double /*avg_value_size_hint*/,
        bool position_independent_encoding) const
{
    ColumnWithDictionary & column_with_dictionary = typeid_cast<ColumnWithDictionary &>(column);
    UInt64 keys_size;
    readIntBinary(keys_size, istr);
    UInt32 indexes_ser_type_val;
    readIntBinary(indexes_ser_type_val, istr);

    auto indexes_ser_type = static_cast<IndexesSerializationType>(indexes_ser_type_val);
    auto expected_index_typ = dataTypeToIndexesSerializationType(*indexes_type);
    if (indexes_ser_type != expected_index_typ)
        throw Exception("Invalid index type for DataTypeWithDictionary: " + toString(indexes_ser_type_val),
                        ErrorCodes::LOGICAL_ERROR);

    auto keys = dictionary_type->createColumn();
    dictionary_type->deserializeBinaryBulkToSingleColumn(*keys, istr, keys_size, 0, position_independent_encoding);
    auto indexes = indexes_type->createColumn();
    indexes_type->deserializeBinaryBulk(*indexes, istr, limit, 0);

    auto idx = column_with_dictionary.getUnique()->uniqueInsertRangeFrom(*keys, 0, keys_size);
    column_with_dictionary.getIndexes()->insertRangeFrom(*idx->index(ColumnPtr(std::move(indexes)), 0), 0, indexes->size());
}

void DataTypeWithDictionary::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    dictionary_type->serializeBinary(field, ostr);
}
void DataTypeWithDictionary::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    dictionary_type->deserializeBinary(field, istr);
}

template <typename ... Args>
void DataTypeWithDictionary::serializeImpl(
        const IColumn & column, size_t row_num, WriteBuffer & ostr,
        DataTypeWithDictionary::SerealizeFunctionPtr<Args ...> func, Args & ... args) const
{
    auto & column_with_dictionary = getColumnWithDictionary(column);
    size_t unique_row_number = column_with_dictionary.getIndexes()->getUInt(row_num);
    (dictionary_type.get()->*func)(*column_with_dictionary.getUnique()->getNestedColumn(), unique_row_number, ostr, std::forward<Args>(args)...);
}

template <typename ... Args>
void DataTypeWithDictionary::deserializeImpl(
        IColumn & column, ReadBuffer & istr,
        DataTypeWithDictionary::DeserealizeFunctionPtr<Args ...> func, Args ... args) const
{
    auto & column_with_dictionary = getColumnWithDictionary(column);
    auto temp_column = column_with_dictionary.getUnique()->cloneEmpty();

    (dictionary_type.get()->*func)(*temp_column, istr, std::forward<Args>(args)...);

    column_with_dictionary.insertFromFullColumn(*temp_column, 0);
}

template <typename ColumnType, typename IndexType>
MutableColumnPtr DataTypeWithDictionary::createColumnImpl() const
{
    return ColumnWithDictionary::create(ColumnUnique<ColumnType, IndexType>::create(dictionary_type),
                                        indexes_type->createColumn());
}

template <typename ColumnType>
MutableColumnPtr DataTypeWithDictionary::createColumnImpl() const
{
    if (typeid_cast<const DataTypeUInt8 *>(indexes_type.get()))
        return createColumnImpl<ColumnType, UInt8>();
    if (typeid_cast<const DataTypeUInt16 *>(indexes_type.get()))
        return createColumnImpl<ColumnType, UInt16>();
    if (typeid_cast<const DataTypeUInt32 *>(indexes_type.get()))
        return createColumnImpl<ColumnType, UInt32>();
    if (typeid_cast<const DataTypeUInt64 *>(indexes_type.get()))
        return createColumnImpl<ColumnType, UInt64>();

    throw Exception("The type of indexes must be unsigned integer, but got " + dictionary_type->getName(),
                    ErrorCodes::LOGICAL_ERROR);
}

struct CreateColumnVector
{
    MutableColumnPtr & column;
    const DataTypeWithDictionary * data_type_with_dictionary;
    const IDataType * type;

    CreateColumnVector(MutableColumnPtr & column, const DataTypeWithDictionary * data_type_with_dictionary,
                       const IDataType * type)
            : column(column), data_type_with_dictionary(data_type_with_dictionary), type(type) {}

    template <typename T, size_t>
    void operator()()
    {
        if (typeid_cast<const DataTypeNumber<T> *>(type))
            column = data_type_with_dictionary->createColumnImpl<ColumnVector<T>>();
    }
};

MutableColumnPtr DataTypeWithDictionary::createColumn() const
{
    auto type = dictionary_type;
    if (type->isNullable())
        type = static_cast<const DataTypeNullable &>(*dictionary_type).getNestedType();

    if (type->isString())
        return createColumnImpl<ColumnString>();
    if (type->isFixedString())
        return createColumnImpl<ColumnFixedString>();
    if (typeid_cast<const DataTypeDate *>(type.get()))
        return createColumnImpl<ColumnVector<UInt16>>();
    if (typeid_cast<const DataTypeDateTime *>(type.get()))
        return createColumnImpl<ColumnVector<UInt32>>();
    if (type->isNumber())
    {
        MutableColumnPtr column;
        TypeListNumbers::forEach(CreateColumnVector(column, this, type.get()));

        if (!column)
            throw Exception("Unexpected numeric type: " + type->getName(), ErrorCodes::LOGICAL_ERROR);

        return column;
    }

    throw Exception("Unexpected dictionary type for DataTypeWithDictionary: " + type->getName(),
                    ErrorCodes::LOGICAL_ERROR);
}

bool DataTypeWithDictionary::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    auto & rhs_with_dictionary = static_cast<const DataTypeWithDictionary &>(rhs);
    return dictionary_type->equals(*rhs_with_dictionary.dictionary_type)
           && indexes_type->equals(*rhs_with_dictionary.indexes_type);
}



static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception("WithDictionary data type family must have two arguments - type of elements and type of indices"
                        , ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<DataTypeWithDictionary>(DataTypeFactory::instance().get(arguments->children[0]),
                                                    DataTypeFactory::instance().get(arguments->children[1]));
}

void registerDataTypeWithDictionary(DataTypeFactory & factory)
{
    factory.registerDataType("WithDictionary", create);
}

}
