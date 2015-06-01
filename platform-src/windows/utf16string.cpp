#include <algorithm>
#include <functional>
#include <cctype>
#include <string>
#include <vector>
#include <ostream>
#include <map>
#include <cstring>
#include <stdexcept>

#include <TeamstudioException.h>
#include <utf16string.h>
#include <utf16stringbuilder.h>
#include "ConvertUTF.h"

const char *utf16string::UTF8 = "UTF8";
const char *utf16string::ISO_8859_1 = "ISO-8859-1";

int trimFunc(int c) { return c <= 32; }

// trim from start
static inline std::u16string &ltrim(std::u16string &s) {
        s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(trimFunc))));
        return s;
}

// trim from end
static inline std::u16string &rtrim(std::u16string &s) {
        s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(trimFunc))).base(), s.end());
        return s;
}

// trim from both ends
static inline std::u16string &trim(std::u16string &s) {
        return ltrim(rtrim(s));
}

class utf16string::StringData
{
public:
	StringData(const char* sz) : m_str(sz), m_pU16(NULL) { }
	StringData(const std::u16string &u16) {
		m_pU16 = new std::u16string(u16);
		if (conversionOK != ConvertUTF16toUTF8(u16, m_str, lenientConversion)) {
			throw TeamstudioException("Unable to convert text to UTF-8");
		}
	}
	StringData(StringData *other) : m_pU16(NULL) {
		m_str = other->m_str;
		if (NULL != other->m_pU16) {
			m_pU16 = new std::u16string(*(other->m_pU16));
		}
	}
	~StringData() {
		clear();
	}

	void clear() {
		if (NULL != m_pU16) {
			delete m_pU16;
			m_pU16 = NULL;
		}
	}

	std::string &str() { return m_str; }

	std::u16string &u16str() {
		if (NULL == m_pU16) {
			m_pU16 = new std::u16string();
			ConvertUTF8toUTF16(m_str, *m_pU16, lenientConversion);
		}

		return *m_pU16;
	}
private:
	std::string m_str;
	std::u16string* m_pU16;
};

utf16string::utf16string()
{
	m_data = new StringData("");
}

utf16string::utf16string(StringData *data)
{
	m_data = data;
}

utf16string::utf16string(const utf16string &str)
{
	m_data = new StringData(str.m_data);
}

utf16string::utf16string(const char *szDefaultEncodedString)
{
	m_data = new StringData(szDefaultEncodedString);
}

utf16string::utf16string(const char *sz, const char *charsetName)
{
	if (NULL == charsetName || charsetName == UTF8 || 0 == strcmp(charsetName, UTF8)) {
		m_data = new StringData(sz);
	}
	else {
		m_data = NULL;
	}
}

utf16string::utf16string(const char *ab, int offset, int length, const char *charsetName)
{
	if (NULL == charsetName || charsetName == UTF8 || 0 == strcmp(charsetName, UTF8)) {
		m_data = new StringData(std::string(ab+offset, length).c_str());
	}
	else {
		m_data = NULL;
	}
}

utf16string::utf16string(const utf16char *ach, int offset, int length)
{
	const char16_t *chars = ((const char16_t*)ach) + offset;
	std::u16string str;
	str.append(chars, length);
	m_data = new StringData(str);
}

utf16string::~utf16string()
{
	delete m_data;
}

utf16string utf16string::valueOf(utf16char c)
{
	return utf16string(&c, 0, 1);
}

utf16string utf16string::valueOf(double d)
{
	throw TeamstudioException("utf16string::valueOf(double) is currently unsupported - needs dtoa");
}

utf16string utf16string::valueOf(long long ll)
{
    char szBuffer[50];
    sprintf_s(szBuffer, 50, "%lld", ll);
    szBuffer[49] = '\0';
    return szBuffer;
}

utf16string utf16string::valueOf(unsigned long dw)
{
    char szBuffer[50];
    sprintf_s(szBuffer, 50, "%lu", dw);
    szBuffer[49] = '\0';
    return szBuffer;
}

utf16string utf16string::valueOf(int i) {
    char szBuffer[50];
    sprintf(szBuffer, "%d", i);
    return utf16string(szBuffer);
}

void utf16string::setData(StringData *data)
{
	delete m_data;
	m_data = data;
}

utf16string &utf16string::operator=(const utf16string &str)
{
	setData(new StringData(str.m_data));
	return *this;
}

utf16string &utf16string::operator+=(const utf16string &str)
{
	//concat utf8 and ditch utf16
	m_data->str() += str.m_data->str();
	m_data->clear();
	return *this;
}

utf16string &utf16string::operator+=(const char *str)
{
	//concat utf8 and ditch utf16
	m_data->str() += str;
	m_data->clear();
	return *this;
}

utf16string &utf16string::operator+=(utf16char c)
{
    return operator+=(utf16string(&c, 0, 1));
}

bool operator==(const utf16string &lhs, const utf16string &rhs)
{
    return lhs.equals(rhs);
}

bool operator==(const char *lhs, const utf16string &rhs)
{
    return rhs.equals(lhs);
}

bool operator==(const utf16string &lhs, const char *rhs)
{
    return lhs.equals(rhs);
}

bool operator!=(const utf16string &lhs, const utf16string &rhs)
{
    return !operator==(lhs, rhs);
}

bool operator!=(const char *lhs, const utf16string &rhs)
{
    return !operator==(lhs, rhs);
}

bool operator!=(const utf16string &lhs, const char *rhs)
{
    return !operator==(lhs, rhs);
}

utf16string operator+(const utf16string &lhs, const utf16string &rhs)
{
	utf16string answer(lhs);
	answer += rhs;
	return answer;
}

utf16string operator+(const char *lhs, const utf16string &rhs)
{
    utf16string answer(lhs);
    answer += rhs;
    return answer;
}

utf16string operator+(const utf16string &lhs, const char *rhs)
{
    utf16string answer(lhs);
    answer += rhs;
    return answer;
}

utf16string operator+(const utf16string &lhs, utf16char rhs)
{
    utf16string answer(lhs);
    answer += rhs;
    return answer;
}

utf16string operator+(utf16char lhs, const utf16string &rhs)
{
    utf16string answer;
    answer += lhs;
    answer += rhs;
    return answer;
}

bool operator<(const utf16string &lhs, const utf16string &rhs)
{
    return lhs.compareTo(rhs) < 0;
}

const char *utf16string::c_str() const
{
	return c_str(UTF8);
}

const char *utf16string::c_str(const char* charsetName) const
{
	if (NULL == charsetName || charsetName == UTF8 || 0 == strcmp(charsetName, UTF8)) {
		return m_data->str().c_str();
	}

	throw TeamstudioException("Unsupported charset for c_str()");
}

bool utf16string::isEmpty() const
{
	//UTF8
	return m_data->str().empty();
}

bool utf16string::equals(const utf16string &other) const
{
	//UTF8
	return m_data->str() == other.m_data->str();
}

bool utf16string::equalsIgnoreCase(const utf16string &other) const
{
	throw TeamstudioException("utf16string::equalsIgnoreCase not supported on this platform");
}

int utf16string::indexOf(const utf16string &other) const
{
	//UTF16
	std::u16string::size_type n = m_data->u16str().find(other.m_data->u16str());
	if (n == std::string::npos) {
		return -1;
	}

	return n;
}

int utf16string::indexOf(const utf16string &other, int start) const
{
	//UTF16
	std::u16string::size_type n = m_data->u16str().find(other.m_data->u16str(), start);
	if (n == std::string::npos) {
		return -1;
	}

	return n;
}

int utf16string::indexOf(utf16char ch) const
{
	//UTF16
	std::u16string::size_type n = m_data->u16str().find(ch);
	if (n == std::string::npos) {
		return -1;
	}

	return n;
}

int utf16string::indexOf(utf16char ch, int start) const
{
	//UTF16
	std::u16string::size_type n = m_data->u16str().find(ch, start);
	if (n == std::string::npos) {
		return -1;
	}

	return n;
}

int utf16string::lastIndexOf(utf16string const &str) const
{
	//UTF16
	std::u16string::size_type n = m_data->u16str().rfind(str.m_data->u16str());
	if (n == std::string::npos) {
		return -1;
	}

	return n;
}

int utf16string::lastIndexOf(utf16char ch) const
{
	//UTF16
	std::u16string::size_type n = m_data->u16str().rfind(ch);
	if (n == std::string::npos) {
		return -1;
	}

	return n;
}

utf16string utf16string::substring(int beginIndex) const
{
	//UTF16
    try {
        std::u16string sub = m_data->u16str().substr(beginIndex);
        utf16string answer;
        answer.setData(new StringData(sub));
        return answer;
    }
    catch (std::out_of_range &e) {
        throw StringIndexOutOfBoundsException(e.what());
    }
}

utf16string utf16string::substring(int beginIndex, int endIndex) const
{
	//UTF16
    try {
        std::u16string sub = m_data->u16str().substr(beginIndex, endIndex - beginIndex);
        utf16string answer;
        answer.setData(new StringData(sub));
        return answer;
    }
    catch (std::out_of_range &e) {
        throw StringIndexOutOfBoundsException(e.what());
    }
}

utf16string utf16string::toLowerCase() const
{
	throw TeamstudioException("utf16string::toLowerCase not supported on this platform");
}

utf16string utf16string::toUpperCase() const
{
	throw TeamstudioException("utf16string::toUpperCase not supported on this platform");
}

bool utf16string::startsWith(const utf16string &prefix) const
{
	//UTF8
	return 0 == m_data->str().compare(0, prefix.m_data->str().length(), prefix.m_data->str());
}

bool utf16string::endsWith(const utf16string &suffix) const
{
	int cbMe = m_data->str().length();
	int cbSuffix = suffix.m_data->str().length();
	return (cbSuffix <= cbMe) && 0 == m_data->str().compare(cbMe - cbSuffix, cbSuffix, suffix.m_data->str());
}

int utf16string::length() const
{
	//UTF16
	return m_data->u16str().length();
}

ByteVectorPtr utf16string::getBytes() const
{
	//UTF8
	return getBytes(UTF8);
}

ByteVectorPtr utf16string::getBytes(const char* encoding) const
{
	//UTF8
	const char* str = m_data->str().c_str();
	ByteVectorPtr answer(new std::vector<unsigned char>(str, str + m_data->str().length()));
	return answer;
}

void utf16string::getChars(int srcBegin, int srcEnd, utf16char *dest, int dstBegin) const
{
	//UTF16
	const char16_t* str = m_data->u16str().c_str();
	utf16char *walk = dest + dstBegin;
	for (int x = srcBegin; x < srcEnd; x++) {
		*walk++ = str[x];
	}
}

utf16string utf16string::trim() const
{
	//UTF16 - chars less than 32
	std::u16string work(m_data->u16str());
	::trim(work);
	utf16string answer;
	answer.setData(new StringData(work));
	return answer;
}

utf16string utf16string::replace(utf16string const& target, utf16string const &replacement) const
{
	//UTF8
	std::string str(m_data->str());
	if (target.m_data->str().empty()) {
		return utf16string(str.c_str());
	}

	size_t start_pos = 0;
	while((start_pos = str.find(target.m_data->str(), start_pos)) != std::string::npos) {
		str.replace(start_pos, target.m_data->str().length(), replacement.m_data->str());
		start_pos += replacement.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
	}

	return utf16string(str.c_str());
}

utf16string utf16string::replace(utf16char target, utf16char replacement) const
{
	//UTF16
	std::u16string answer(m_data->u16str());
	std::replace(answer.begin(), answer.end(), target, replacement);
	utf16string str;
	str.setData(new StringData(answer));
	return str;
}

utf16char utf16string::charAt(int index) const
{
	//UTF16
	return m_data->u16str().at(index);
}

int utf16string::compareTo(const utf16string &other) const
{
	//UTF8
	return m_data->str().compare(other.m_data->str());
}

std::size_t utf16string::hash() const
{
	std::hash<std::string> hash_fn;
	return hash_fn(m_data->str());
}

void *utf16string::getPlatformString() const
{
	return NULL;
}

std::ostream &operator<<(std::ostream &stream, const utf16string &str)
{
	return stream << str.c_str();
}

bool utf16character::isLetter(utf16char c)
{
	throw TeamstudioException("utf16character::isLetter not supported on this platform");
}

