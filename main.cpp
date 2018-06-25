#include <boost/hana.hpp>
#include <pqxx/pqxx>

#include <iostream>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

#include <map>
#include <string>
#include <vector>

namespace hana = boost::hana;

struct Member {
    BOOST_HANA_DEFINE_STRUCT(Member,
    (int, id),
    (std::string, name)
    );
};

struct Location {
    BOOST_HANA_DEFINE_STRUCT(Location,
    (int, longitude),
    (int, latitude),
    (std::string, address)
    );
};

struct Group {
    BOOST_HANA_DEFINE_STRUCT(Group,
    (int, groupid),
    (std::string, group_name),
    (Location, location),
    (std::vector<Member>, members)
    );
};

struct SearchOptions {
    BOOST_HANA_DEFINE_STRUCT(SearchOptions,
    (std::string, start_date),
    (std::string, end_date),
    (std::string, start_time),
    (std::string, end_time),
    (int, radius),
    (int, center_longitude),
    (int, center_latitude),
    (std::string, mode)
    );
};

template<typename T>
T get(pqxx::field const& field)
{
    return field.as<T>();
}

template<typename T>
struct Val
{
    using Type = T;
    rapidjson::Value const& v;
};

/**
 * Parse json value as int
 */
int parseValue(Val<int> const& value)
{
    return value.v.GetInt();
}

/**
 * Parse value value as std::string
 */
std::string parseValue(Val<std::string> const& value)
{
    return value.v.GetString();
}

/**
 * Parse json value value as vector of elements.
 */
template<typename T>
std::vector<T> parseValue(Val<std::vector<T>> const& value)
{
    std::vector<T> objects;
    for (size_t i = 0; i < value.v.Size(); ++i)
    {
        objects.push_back(parseValue(Val<T>{value.v[i]}));
    }

    return objects;
}

/**
 * \brief parse json value as a object.
 */
template<typename T>
T parseValue(Val<T> const& value)
{
    T t;
    boost::hana::for_each(boost::hana::keys(t), [&](auto key) {
                auto &member = boost::hana::at_key(t, key);
                using ValueType = typename std::remove_reference<decltype(member)>::type;
                member = parseValue(Val<ValueType>{value.v[boost::hana::to<char const*>(key)]});
            });

    return t;
};

using JsonWriter = rapidjson::Writer<rapidjson::StringBuffer>;

void packValue(std::string const& value, JsonWriter & writer)
{
    writer.String(value.c_str());
}

void packValue(int value, JsonWriter & writer)
{
    writer.Int(value);
}

template<typename T>
void packValue(std::vector<T> const& t, JsonWriter & writer)
{
    writer.StartArray();
    for (auto const& e : t)
    {
        packValue(e, writer);
    }
    writer.EndArray();
}

template<typename T>
void packValue(T const& t, JsonWriter & writer)
{
    writer.StartObject();

    boost::hana::for_each(boost::hana::keys(t), [&](auto key) {
                auto &member = boost::hana::at_key(t, key);

                char const * json_key = boost::hana::to<char const*>(key);
                writer.Key(json_key);
                packValue(member, writer);
            });

    writer.EndObject();
}

template<typename T>
T fromJson(std::string const& s)
{
    rapidjson::Document doc;
    doc.Parse(s.c_str());
    return parseValue(Val<T>{doc});
}

template<typename T>
std::string toJson(T const& value)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    packValue(value, writer);

    return buffer.GetString();
}

auto make_db_executor(pqxx::transaction_base & trx_base, std::string const& query)
{
    return [&](auto... args)
    {
        trx_base.exec_prepared(query, args...);
    };
}

template<typename T>
void runQuery(pqxx::transaction_base & trx_base, std::string const& query, T const& obj)
{
    auto new_tuple = hana::transform(hana::to_tuple(obj), [](auto x) { return hana::second(x); });
    boost::hana::unpack(new_tuple, make_db_executor(trx_base, "add_user"));
}

int main()
{
    Group group{1,"Best group", Location{1,1,"sinsenveien 7"}, {Member{0, "Jens"}, Member{1, "Harald"}}};

    auto stringified_group = toJson(group);
    std::cout << stringified_group << '\n';

    auto parsed_group = fromJson<Group>(stringified_group);

    pqxx::connection c("dbname=test");
    c.prepare("add_user", "insert into users(id, name) values($1, $2)");

    auto work = pqxx::work(c);

    for (auto const& member : parsed_group.members)
    {
        runQuery(work, "add_user", member);
    }
    work.commit();
}
