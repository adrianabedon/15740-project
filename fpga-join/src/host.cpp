
#include <stdlib.h>
#include <fstream>
#include <iostream>
#include "kml_join.hpp"
#include "xcl2.hpp"


static const int DATA_SIZE = 4096;

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
  size_t input_size_in_bytes = DATA_SIZE * (sizeof(int)*2);
  size_t output_size_in_bytes = DATA_SIZE * (sizeof(int)*3);

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

  int narg = 0;
  kml_join.setArg(narg++, buffer_input1);
  kml_join.setArg(narg++, buffer_input2);
  kml_join.setArg(narg++, buffer_result);
  kml_join.setArg(narg++, DATA_SIZE);
  kml_join.setArg(narg++, DATA_SIZE);

  // We then need to map our OpenCL buffers to get the pointers
  input_tuple_t *ptr_input1 = (input_tuple_t *)q.enqueueMapBuffer(buffer_input1, CL_TRUE, CL_MAP_WRITE, 0, input_size_in_bytes);
  input_tuple_t *ptr_input2 = (input_tuple_t *)q.enqueueMapBuffer(buffer_input2, CL_TRUE, CL_MAP_WRITE, 0, input_size_in_bytes);
  output_tuple_t *ptr_result = (output_tuple_t *)q.enqueueMapBuffer(buffer_result, CL_TRUE, CL_MAP_READ, 0, output_size_in_bytes);

  // setting input data
  for (int i = 0; i < DATA_SIZE; i++)
  {
    ptr_input1[i].rid = i;
    ptr_input1[i].key = i;
    ptr_input2[i].rid = DATA_SIZE + i;
    ptr_input2[i].key = i;
  }

  // Data will be migrated to kernel space
  q.enqueueMigrateMemObjects({buffer_input1, buffer_input2}, 0 /* 0 means from host*/);

  // Launch the Kernel
  q.enqueueTask(kml_join);

  // The result of the previous kernel execution will need to be retrieved in
  // order to view the results. This call will transfer the data from FPGA to
  // source_results vector
  q.enqueueMigrateMemObjects({buffer_result}, CL_MIGRATE_MEM_OBJECT_HOST);

  q.finish();

  // Verify the result
  // int match = 0;
  // for (int i = 0; i < DATA_SIZE; i++)
  // {
  //   int host_result = ptr_a[i] + ptr_b[i];
  //   if (ptr_result[i] != host_result)
  //   {
  //     printf(error_message.c_str(), i, host_result, ptr_result[i]);
  //     match = 1;
  //     break;
  //   }
  // }
  //output result to file?

  q.enqueueUnmapMemObject(buffer_input1, ptr_input1);
  q.enqueueUnmapMemObject(buffer_input2, ptr_input2);
  q.enqueueUnmapMemObject(buffer_result, ptr_result);
  q.finish();

  // std::cout << "TEST " << (match ? "FAILED" : "PASSED") << std::endl;
  std::cout << "HOLY SHIT" << std::endl;
  return EXIT_SUCCESS;
}
