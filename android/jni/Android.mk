LOCAL_PATH := $(call my-dir)/../..
include $(CLEAR_VARS)

LOCAL_CPP_FEATURES := rtti exceptions
LOCAL_C_INCLUDES += $(LOCAL_PATH)/src $(LOCAL_PATH)/src/datastore
LOCAL_CPPFLAGS += -std=c++11 -D__STDC_LIMIT_MACROS -D__GXX_EXPERIMENTAL_CXX0X__ -DUNIX -DMONGO_USE_LONG_LONG_INT
LOCAL_CFLAGS += -DMONGO_USE_LONG_LONG_INT -std=c99

LOCAL_MODULE    := lowladb
FILE_LIST := $(wildcard $(LOCAL_PATH)/src/*.cpp) \
             $(wildcard $(LOCAL_PATH)/src/bson/*.c) \
             $(wildcard $(LOCAL_PATH)/src/json/*.cpp) \
             $(wildcard $(LOCAL_PATH)/src/datastore/*.c) \
             $(wildcard $(LOCAL_PATH)/platform-src/android/*.cpp)
			 
LOCAL_SRC_FILES := $(FILE_LIST:$(LOCAL_PATH)/%=%)

LOCAL_EXPORT_C_INCLUDES := $(LOCAL_PATH)/src $(LOCAL_PATH)/platform-src/android

include $(BUILD_STATIC_LIBRARY)
