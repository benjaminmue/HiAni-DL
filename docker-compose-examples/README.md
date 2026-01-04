# Docker Compose Examples

This directory contains platform-specific docker-compose.yml examples to help you get started with HiAni-DL on different operating systems.

## Quick Start

1. **Choose your platform:**
   - `macos/` - For macOS (Intel and Apple Silicon)
   - `linux/` - For Linux distributions
   - `windows/` - For Windows

2. **Copy the example:**
   ```bash
   # From the project root
   cp docker-compose-examples/YOUR_PLATFORM/docker-compose.yml ./docker-compose.yml
   ```

3. **Customize the paths:**
   Edit the `docker-compose.yml` file and change:
   - Download path (where anime files will be saved)
   - Timezone (to match your location)

4. **Start the container:**
   ```bash
   docker-compose up -d
   ```

## Platform-Specific Notes

### macOS

**Architecture Support:**
- **Apple Silicon (M1/M2/M3/M4/M5)**: Uses ARM64 architecture
  - Automatically pulls ARM64 image variant
  - Uses Chromium browser (configured as fallback in Dockerfile)
- **Intel Macs**: Uses AMD64 architecture
  - Pulls standard AMD64 image
  - Uses Google Chrome

The multi-arch Docker image automatically selects the correct architecture for your Mac.

### Linux

Works on both ARM64 and AMD64 architectures. The Docker image will automatically pull the correct variant for your system.

### Windows

Uses AMD64 architecture. Note that Windows paths in Docker can use either:
- Forward slashes: `C:/Users/YourUsername/Downloads`
- Backslashes (escaped): `C:\\Users\\YourUsername\\Downloads`

We recommend forward slashes for simplicity.

## See Also

- [Quick Start Guide](../../../wiki/Quick-Start-Guide) - Detailed setup instructions
- [Docker Setup](../../../wiki/Docker-Setup) - Advanced configuration options
- [Main README](../README.md) - Project overview
