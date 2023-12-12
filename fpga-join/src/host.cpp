
#include <stdlib.h>
#include <fstream>
#include <iostream>
#include "kml_join.hpp"
#include "xcl2.hpp"
#include "check_result.hpp"

static const int NUM_INPUT_TUPLES = NUM_TUPLES;

static const std::string error_message =
    "Error: Result mismatch:\n"
    "i = %d CPU result = %d Device result = %d\n";

int main(int argc, char *argv[])
{
  // TARGET_DEVICE macro needs to be passed from gcc command line
  if (argc != 2)
  {
    std::cout << "Usage: " << argv[0] << " <xclbin>" << std::endl;
    return EXIT_FAILURE;
  }

  char *xclbinFilename = argv[1];

  // Compute the size of array in bytes
  size_t input_size_in_bytes = NUM_INPUT_TUPLES * (sizeof(input_tuple_t));
  size_t output_size_in_bytes = NUM_INPUT_TUPLES * (sizeof(output_tuple_t));

  // Creates a vector of DATA_SIZE elements with an initial value of 10 and 32
  // using customized allocator for getting buffer alignment to 4k boundary

  std::vector<cl::Device> devices;
  cl::Device device;
  std::vector<cl::Platform> platforms;
  bool found_device = false;

  // traversing all Platforms To find Xilinx Platform and targeted
  // Device in Xilinx Platform
  cl::Platform::get(&platforms);
  for (size_t i = 0; (i < platforms.size()) & (found_device == false); i++)
  {
    cl::Platform platform = platforms[i];
    std::string platformName = platform.getInfo<CL_PLATFORM_NAME>();
    if (platformName == "Xilinx")
    {
      devices.clear();
      platform.getDevices(CL_DEVICE_TYPE_ACCELERATOR, &devices);
      if (devices.size())
      {
        device = devices[0];
        found_device = true;
        break;
      }
    }
  }
  if (found_device == false)
  {
    std::cout << "Error: Unable to find Target Device "
              << device.getInfo<CL_DEVICE_NAME>() << std::endl;
    return EXIT_FAILURE;
  }

  // Creating Context and Command Queue for selected device
  cl::Context context(device);
  cl::CommandQueue q(context, device, CL_QUEUE_PROFILING_ENABLE);

  // Load xclbin
  std::cout << "========================================================" << std::endl;
  std::cout << "Loading: '" << xclbinFilename << "'\n";
  std::ifstream bin_file(xclbinFilename, std::ifstream::binary);
  bin_file.seekg(0, bin_file.end);
  unsigned nb = bin_file.tellg();
  bin_file.seekg(0, bin_file.beg);
  char *buf = new char[nb];
  bin_file.read(buf, nb);

  // Creating Program from Binary File
  cl::Program::Binaries bins;
  bins.push_back({buf, nb});
  devices.resize(1);
  cl::Program program(context, devices, bins);

  // This call will get the kernel object from program. A kernel is an
  // OpenCL function that is executed on the FPGA.
  cl::Kernel kml_join(program, "kml_join");

  // These commands will allocate memory on the Device. The cl::Buffer objects can
  // be used to reference the memory locations on the device.
  cl::Buffer buffer_input1(context, CL_MEM_READ_ONLY, input_size_in_bytes);
  cl::Buffer buffer_input2(context, CL_MEM_READ_ONLY, input_size_in_bytes);
  cl::Buffer buffer_result(context, CL_MEM_WRITE_ONLY, output_size_in_bytes);
  cl::Buffer buffer_result_size(context, CL_MEM_WRITE_ONLY, sizeof(int));

  // We then need to map our OpenCL buffers to get the pointers
  input_tuple_t *ptr_input1 = (input_tuple_t *)q.enqueueMapBuffer(buffer_input1, CL_TRUE, CL_MAP_WRITE, 0, input_size_in_bytes);
  input_tuple_t *ptr_input2 = (input_tuple_t *)q.enqueueMapBuffer(buffer_input2, CL_TRUE, CL_MAP_WRITE, 0, input_size_in_bytes);
  output_tuple_t *ptr_result = (output_tuple_t *)q.enqueueMapBuffer(buffer_result, CL_TRUE, CL_MAP_READ, 0, output_size_in_bytes);
  int *result_size = (int *)q.enqueueMapBuffer(buffer_result_size, CL_TRUE, CL_MAP_READ, 0, sizeof(int));

  // setting input data
  for (int i = 0; i < NUM_INPUT_TUPLES; i++)
  {
    ptr_input1[i].rid = i;
    ptr_input1[i].key = i;
    ptr_input2[i].rid = NUM_INPUT_TUPLES + i;
    ptr_input2[i].key = i;
  }

  // Data will be migrated to kernel space
  q.enqueueMigrateMemObjects({buffer_input1, buffer_input2}, 0 /* 0 means from host*/);

  q.finish();

  cl::Event event;
  uint64_t nstimestart, nstimeend;

  int narg = 0;
  kml_join.setArg(narg++, buffer_input1);
  kml_join.setArg(narg++, buffer_input2);
  kml_join.setArg(narg++, buffer_result);
  kml_join.setArg(narg++, buffer_result_size);
  kml_join.setArg(narg++, NUM_INPUT_TUPLES);
  kml_join.setArg(narg++, NUM_INPUT_TUPLES);

  // Launch the Kernel
  q.enqueueTask(kml_join, NULL, &event);

  q.finish();

  event.getProfilingInfo<uint64_t>(CL_PROFILING_COMMAND_START, &nstimestart);
  event.getProfilingInfo<uint64_t>(CL_PROFILING_COMMAND_END, &nstimeend);
  auto matmul_time = nstimeend - nstimestart;

  auto tuples_per_sec = (NUM_INPUT_TUPLES) / (matmul_time / 1000000000.0);

  std::cout << "tuples per sec: " << tuples_per_sec << std::endl;
  // The result of the previous kernel execution will need to be retrieved in
  // order to view the results. This call will transfer the data from FPGA to
  // source_results vector
  q.enqueueMigrateMemObjects({buffer_result}, CL_MIGRATE_MEM_OBJECT_HOST);
  q.enqueueMigrateMemObjects({buffer_result_size}, CL_MIGRATE_MEM_OBJECT_HOST);
  q.finish();

  std::cout << "result size: " << *result_size << std::endl;

  int retval = check_result(ptr_input1, ptr_input2, ptr_result, NUM_INPUT_TUPLES, NUM_INPUT_TUPLES, *result_size);
  if (retval)
  {
    std::cout << "xxxxxxxxx\nTEST FAILED\nxxxxxxxxx" << std::endl;
    return EXIT_FAILURE;
  }

  q.enqueueUnmapMemObject(buffer_input1, ptr_input1);
  q.enqueueUnmapMemObject(buffer_input2, ptr_input2);
  q.enqueueUnmapMemObject(buffer_result, ptr_result);
  q.finish();

  // std::cout << "TEST " << (match ? "FAILED" : "PASSED") << std::endl;
  std::cout << "*******\nSUCCESS\n*******" << std::endl;
  return EXIT_SUCCESS;
}
