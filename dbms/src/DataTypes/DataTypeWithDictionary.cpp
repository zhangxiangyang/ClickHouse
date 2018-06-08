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

void DataTypeWithDictionary::enumerateStreams(StreamCallback callback, SubstreamPath path) const
{
    path.push_back(Substream::DictionaryKeys);
    dictionary_type->enumerateStreams(callback, path);
    path.back() = Substream::DictionaryIndexes;
    indexes_type->enumerateStreams(callback, path);
}

enum class KeysSerializationVersion
{
    /// Write keys as full column. No indexes is written. Structure:
    ///   <name>.dict.bin : [version - 32 bits][keys]
    ///   <name>.dict.mrk : [marks for keys]
    FullColumn = 0,
    /// Write all keys in serializePostfix and read in deserializePrefix.
    ///   <name>.dict.bin : [version - 32 bits][indexes type - 32 bits][keys]
    ///   <name>.bin : [indexes]
    ///   <name>.mrk : [marks for indexes]
    SingleDictionary,
    /// Write distinct set of keys for each granule. Structure:
    ///   <name>.dict.bin : [version - 32 bits][indexes type - 32 bits][keys]
    ///   <name>.dict.mrk : [marks for keys]
    ///   <name>.bin : [indexes]
    ///   <name>.mrk : [marks for indexes]
    DictionaryPerGranule,
};

enum class IndexesSerializationType
{
    UInt8 = 0,
    UInt16,
    UInt32,
    UInt64
};

IndexesSerializationType dataTypeToIndexesSerializationType(const IDataType & type)
{
    if (typeid_cast<const DataTypeUInt8 *>(&type))
        return IndexesSerializationType::UInt8;
    if (typeid_cast<const DataTypeUInt16 *>(&type))
        return IndexesSerializationType::UInt16;
    if (typeid_cast<const DataTypeUInt32 *>(&type))
        return IndexesSerializationType::UInt32;
    if (typeid_cast<const DataTypeUInt64 *>(&type))
        return IndexesSerializationType::UInt64;

    throw Exception("Invalid DataType for IndexesSerializationType. Expected UInt*, got " + type.getName(),
                    ErrorCodes::LOGICAL_ERROR);
}

struct SerializeBinaryBulkStateWithDictionary : public IDataType::SerializeBinaryBulkState
{
    IDataType::SerializeBinaryBulkStatePtr keys_state;
    KeysSerializationVersion key_version;

    explicit SerializeBinaryBulkStateWithDictionary(
            IDataType::SerializeBinaryBulkStatePtr && keys_state, KeysSerializationVersion key_version)
    : keys_state(std::move(keys_state)), key_version(key_version) {}
};

struct SerializeStateWithDictionaryPerGranule : public SerializeBinaryBulkStateWithDictionary
{
    IndexesSerializationType index_type;
    MutableColumnPtr dictionary_keys;

    SerializeStateWithDictionaryPerGranule(
        IDataType::SerializeBinaryBulkStatePtr && keys_state
        , KeysSerializationVersion key_version
        , IndexesSerializationType index_type)
        : SerializeBinaryBulkStateWithDictionary(std::move(keys_state), key_version)
        , index_type(index_type) {}
};

struct DeserializeBinaryBulkStateWithDictionary : public IDataType::DeserializeBinaryBulkState
{
    IDataType::DeserializeBinaryBulkStatePtr keys_state;
    KeysSerializationVersion key_version;

    explicit DeserializeBinaryBulkStateWithDictionary(
            IDataType::DeserializeBinaryBulkStatePtr && keys_state, KeysSerializationVersion key_version)
            : keys_state(std::move(keys_state)), key_version(key_version) {}
};

struct DeserializeStateWithDictionaryPerGranule : public DeserializeBinaryBulkStateWithDictionary
{
    IndexesSerializationType index_type;

    UInt64 num_rows_to_read_until_next_index = 0;
    ColumnPtr keys_in_current_granule;

    DeserializeStateWithDictionaryPerGranule(
        IDataType::DeserializeBinaryBulkStatePtr && keys_state
        , KeysSerializationVersion key_version
        , IndexesSerializationType index_type)
        : DeserializeBinaryBulkStateWithDictionary(std::move(keys_state), key_version)
        , index_type(index_type) {}
};


IDataType::SerializeBinaryBulkStatePtr
DataTypeWithDictionary::serializeBinaryBulkStatePrefix(OutputStreamGetter getter, SubstreamPath path) const
{
    path.push_back(Substream::DictionaryKeys);
    auto * stream = getter(path);

    if (!stream)
        throw Exception("Got empty stream in DataTypeWithDictionary::serializeBinaryBulkStatePrefix",
                        ErrorCodes::LOGICAL_ERROR);

    /// TODO: select serialization method here.
    KeysSerializationVersion keys_ser_version = KeysSerializationVersion::DictionaryPerGranule;
    IndexesSerializationType indexes_ser_type = dataTypeToIndexesSerializationType(*indexes_type);

    auto keys_ser_version_val = static_cast<UInt32>(keys_ser_version);
    auto indexes_ser_type_val = static_cast<UInt32>(indexes_ser_type);

    writeIntBinary(keys_ser_version_val, *stream);
    writeIntBinary(indexes_ser_type_val, *stream);

    path.push_back(IDataType::Substream::DictionaryKeys);
    auto keys_state = dictionary_type->serializeBinaryBulkStatePrefix(getter, path);
    return std::make_shared<SerializeStateWithDictionaryPerGranule>(
            std::move(keys_state), keys_ser_version, indexes_ser_type);
}


void DataTypeWithDictionary::serializeBinaryBulkStateSuffix(
    const SerializeBinaryBulkStatePtr & state,
    OutputStreamGetter getter,
    SubstreamPath path,
    bool position_independent_encoding) const
{
    if (!state)
        throw Exception("Got empty state for DataTypeWithDictionary::serializeBinaryBulkStateSuffix",
                        ErrorCodes::LOGICAL_ERROR);

    auto * ser_state = dynamic_cast<SerializeBinaryBulkStateWithDictionary *>(state.get());
    if (!ser_state)
        throw Exception("Invalid state for DataTypeWithDictionary::serializeBinaryBulkStateSuffix. Expected "
                        + demangle(typeid(SerializeBinaryBulkStateWithDictionary).name()) + ", got "
                        + demangle(typeid(*state).name()), ErrorCodes::LOGICAL_ERROR);

    path.push_back(Substream::DictionaryKeys);

    switch (ser_state->key_version)
    {
        case KeysSerializationVersion::DictionaryPerGranule:
        {
            auto * state_per_granule = typeid_cast<SerializeStateWithDictionaryPerGranule *>(state.get());
            if (!state_per_granule)
                throw Exception("Invalid state for DataTypeWithDictionary::serializeBinaryBulkStateSuffix. Expected "
                                + demangle(typeid(SerializeStateWithDictionaryPerGranule).name()) + ", got "
                                + demangle(typeid(*state).name()), ErrorCodes::LOGICAL_ERROR);

            if (state_per_granule->dictionary_keys)
            {
                auto * stream = getter(path);
                if (!stream)
                    throw Exception("Got empty stream in DataTypeWithDictionary::serializeBinaryBulkStateSuffix",
                                    ErrorCodes::LOGICAL_ERROR);

                ColumnPtr column = std::move(state_per_granule->dictionary_keys);
                dictionary_type->serializeBinaryBulkWithMultipleStreams(
                        *column, getter, 0, column->size(),
                        position_independent_encoding, path, state_per_granule->keys_state);
            }
            break;
        }
        default:
            throw Exception("Invalid KeysSerializationVersion for DataTypeWithDictionary", ErrorCodes::LOGICAL_ERROR);
    }

    auto keys_state = dictionary_type->serializeBinaryBulkStateSuffix(
            ser_state->keys_state, getter, path, position_independent_encoding);
}


IDataType::DeserializeBinaryBulkStatePtr
DataTypeWithDictionary::deserializeBinaryBulkStatePrefix(InputStreamGetter getter, SubstreamPath path) const
{
    path.push_back(Substream::DictionaryKeys);
    auto * stream = getter(path);

    if (!stream)
        throw Exception("Got empty stream in DataTypeWithDictionary::deserializeBinaryBulkStatePrefix",
                        ErrorCodes::LOGICAL_ERROR);

    UInt32 keys_ser_version_val;
    readIntBinary(keys_ser_version_val, *stream);
    auto keys_ser_version = static_cast<KeysSerializationVersion>(keys_ser_version_val);

    UInt32 indexes_ser_type_val;
    if (keys_ser_version == KeysSerializationVersion::DictionaryPerGranule)
        readIntBinary(indexes_ser_type_val, *stream);

    path.push_back(IDataType::Substream::DictionaryKeys);
    auto keys_state = dictionary_type->deserializeBinaryBulkStatePrefix(getter, path);

    switch (keys_ser_version)
    {
        case KeysSerializationVersion::DictionaryPerGranule:
        {
            auto indexes_ser_type = static_cast<IndexesSerializationType>(indexes_ser_type_val);
            auto expected_index_typ = dataTypeToIndexesSerializationType(*indexes_type);
            if (indexes_ser_type != expected_index_typ)
                throw Exception("Invalid index type for DataTypeWithDictionary: " + toString(indexes_ser_type_val),
                                ErrorCodes::LOGICAL_ERROR);

            return std::make_shared<DeserializeStateWithDictionaryPerGranule>(
                    std::move(keys_state), keys_ser_version, indexes_ser_type);
        }
        default:
            throw Exception("Invalid KeysSerializationVersion for DataTypeWithDictionary.", ErrorCodes::LOGICAL_ERROR);
    }
}


void DataTypeWithDictionary::deserializeBinaryBulkStateSuffix(const DeserializeBinaryBulkStatePtr & state) const
{
    if (!state)
        throw Exception("Got empty state for DataTypeWithDictionary::deserializeBinaryBulkStateSuffix",
                        ErrorCodes::LOGICAL_ERROR);

    auto * deser_state = dynamic_cast<DeserializeBinaryBulkStateWithDictionary *>(state.get());
    if (!deser_state)
        throw Exception("Invalid state for DataTypeWithDictionary::deserializeBinaryBulkStateSuffix. Expected "
                        + demangle(typeid(DeserializeBinaryBulkStateWithDictionary).name()) + ", got "
                        + demangle(typeid(*state).name()), ErrorCodes::LOGICAL_ERROR);

    switch (deser_state->key_version)
    {
        case KeysSerializationVersion::DictionaryPerGranule:
        {
            break;
        }
        default:
            throw Exception("Invalid KeysSerializationVersion for DataTypeWithDictionary", ErrorCodes::LOGICAL_ERROR);
    }

    dictionary_type->serializeBinaryBulkStateSuffix(deser_state->keys_state);
}


void DataTypeWithDictionary::serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        OutputStreamGetter getter,
        size_t offset,
        size_t limit,
        bool position_independent_encoding,
        SubstreamPath path,
        const SerializeBinaryBulkStatePtr & state) const
{
    const ColumnWithDictionary & column_with_dictionary = typeid_cast<const ColumnWithDictionary &>(column);

    auto * dict_state = dynamic_cast<SerializeBinaryBulkStateWithDictionary *>(state.get());
    if (!dict_state)
        throw Exception("Invalid SerializeBinaryBulkState.", ErrorCodes::LOGICAL_ERROR);

    if (dict_state->key_version != KeysSerializationVersion::DictionaryPerGranule)
        throw Exception("Unsupported KeysSerializationVersion for DataTypeWithDictionary", ErrorCodes::LOGICAL_ERROR);

    auto * dict_per_granule_state = typeid_cast<SerializeStateWithDictionaryPerGranule *>(state.get());
    if (!dict_per_granule_state)
        throw Exception("Invalid SerializeBinaryBulkState.", ErrorCodes::LOGICAL_ERROR);

    size_t max_limit = column.size() - offset;
    limit = limit ? std::min(limit, max_limit) : max_limit;

    path.push_back(Substream::DictionaryIndexes);
    if (auto stream = getter(path))
    {
        const auto & indexes = column_with_dictionary.getIndexesPtr();
        const auto & keys = column_with_dictionary.getUnique()->getNestedColumn();
        MutableColumnPtr sub_index = (*indexes->cut(offset, limit)).mutate();
        ColumnPtr unique_indexes = makeSubIndex(*sub_index);
        /// unique_indexes->index(sub_index) == indexes[offset:offset + limit]
        MutableColumnPtr used_keys = (*keys->index(unique_indexes, 0)).mutate();
        /// (used_keys, sub_index) is ColumnWithDictionary for range [offset:offset + limit]

        if (dict_per_granule_state->dictionary_keys)
        {
            auto * column_unique = typeid_cast<IColumnUnique *>(dict_per_granule_state->dictionary_keys.get());
            if (!column_unique)
                throw Exception("Invalid column type for SerializeStateWithDictionaryPerGranule. Expected: "
                                + demangle(typeid(IColumnUnique).name()) + ", got "
                                + demangle(typeid(*dict_per_granule_state->dictionary_keys).name()),
                                ErrorCodes::LOGICAL_ERROR);

            auto global_indexes = column_unique->uniqueInsertRangeFrom(*used_keys, 0, used_keys->size());
            sub_index = (*global_indexes->index(std::move(sub_index), 0)).mutate();
        }
        else
            dict_per_granule_state->dictionary_keys = std::move(used_keys);

        UInt64 used_keys_size = used_keys->size();
        writeIntBinary(used_keys_size, *stream);

        UInt64 indexes_size = sub_index->size();
        writeIntBinary(indexes_size, *stream);

        path.back() = Substream::DictionaryKeys;

        bool keys_was_written = false;
        auto proxy_getter = [&getter](SubstreamPath stream_path) -> WriteBuffer *
        {
            auto * buffer = getter(stream_path);
            if (buffer)
                keys_was_written = true;

            return buffer;
        };

        dictionary_type->serializeBinaryBulkWithMultipleStreams(*dict_per_granule_state->dictionary_keys, proxy_getter, 0, 0,
                                                                position_independent_encoding, path, dict_state->keys_state);
        if (keys_was_written)
            dict_per_granule_state->dictionary_keys = nullptr;

        indexes_type->serializeBinaryBulk(*sub_index, *stream, 0, limit);
    }
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
                dict_state_per_granule->keys_in_current_granule = readDict(num_keys);
            }

            size_t num_rows_to_read = std::min(limit, dict_state_per_granule->num_rows_to_read_until_next_index);
            readIndexes(stream, dict_state_per_granule->keys_in_current_granule, num_rows_to_read);
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
        double avg_value_size_hint,
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
    column_with_dictionary.getIndexes()->insertRangeFrom(*idx->index(indexes, 0), 0, indexes->size());
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
