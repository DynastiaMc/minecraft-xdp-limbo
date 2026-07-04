# Customizing Server Brand

To use a custom server brand, you must maintain your own fork of the repository and recompile the software. Brand customization is not supported via configuration files to ensure PicoLimbo's identity remains consistent.

> [!WARNING]
> Do not submit Pull Requests to add brand configuration. All such PRs will be automatically rejected.

## Option 1: Using GitHub (No local setup required)

This is the easiest method. It uses GitHub's web interface to edit code and GitHub Actions to compile the binary.

### 1. Edit the Brand
1. [Fork the repository](https://github.com/Quozul/PicoLimbo/fork) to your GitHub account.
2. Navigate to [`pico_limbo/src/server_brand.rs`](https://github.com/Quozul/PicoLimbo/blob/master/pico_limbo/src/server_brand.rs).
3. Click the **✏️ (Edit)** button.
4. Change the `SERVER_BRAND` constant:
   ```rust
   pub const SERVER_BRAND: &str = "MyServer"; // Replace PicoLimbo with your name
   ```
5. Click **Commit changes**.
6. Select **Commit directly to the master branch** and enter a commit message (e.g., `chore: customize brand`).

### 2. Build the Binary
1. In your fork, go to the **Actions** tab.
2. Select the **Release** workflow from the left sidebar.
3. Click the **Run workflow** dropdown.
4. Enter a version number starting with "v" (e.g., `v2.0.0+my-brand`). This prefix is required by the workflow.
5. Wait for the workflow to finish (indicated by a green checkmark).
6. Visit the **Releases** page in your fork to download the new binary.

*Note: The build process typically takes 5–10 minutes.*

## Option 2: Building Locally

Use this method if you prefer to manage the build process on your own machine.

1. **Fork and Clone** the repository to your local machine:
   ```bash
   git clone https://github.com/YOUR_USERNAME/PicoLimbo.git
   cd PicoLimbo
   ```
2. **Edit the Brand**: Open `pico_limbo/src/server_brand.rs` in your editor and change the `SERVER_BRAND` constant.
3. **Compile**:
   ```bash
   cargo build --release
   ```
4. **Locate Binary**: The compiled binary will be located at `target/release/pico_limbo`.

For detailed instructions on setting up a Rust environment, see [Compiling from Source](../about/installation.html#compiling-from-source).

## Verification

To confirm the brand has been applied:
1. Connect to your server with a Minecraft client.
2. Press **F3** to open the debug screen.
3. Look for the brand string in the top-left corner (available in versions 1.13+).

## Maintenance

- **Upstream Updates**: When the main PicoLimbo repository is updated, you must merge those changes into your fork and recompile to stay up to date.
- **Technical Details**: The brand is a single constant in `pico_limbo/src/server_brand.rs` that is automatically applied to both the Configuration Handler (v1.13–1.20.1) and the Login Handler (v1.20.2+).
