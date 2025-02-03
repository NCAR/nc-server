
#ifndef _CStringCache_h_
#define _CStringCache_h_

#include <cstring>
#include <string>
#include <vector>
#include <memory>


/**
 * Cache char arrays in a vector of smart pointers, so all the char strings
 * are deleted when the cache goes out of scope.
 */
class CStringCache
{
public:
    CStringCache()
    {}

    std::vector< std::unique_ptr<char[]> > strings{};

    char*
    cache(const std::string& text)
    {
        strings.emplace_back(new char[text.length()+1]);
        return strcpy(strings.back().get(), text.c_str());
    }
};

#endif
