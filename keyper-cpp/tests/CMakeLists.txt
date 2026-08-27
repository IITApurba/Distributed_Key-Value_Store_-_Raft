add_executable(keyper_tests test_main.cpp)
target_link_libraries(keyper_tests PRIVATE keyper_core)
add_test(NAME keyper_tests COMMAND keyper_tests)
