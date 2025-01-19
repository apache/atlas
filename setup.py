#!/usr/bin/env python3

import os
import subprocess
import sys
import platform
import time





def run_command(cmd, cwd=None, print_output=True, raise_on_error=True):
    """
    Runs a shell command with transparency.

    :param cmd: Command string or list of strings (e.g. ['brew', 'install', 'wget'])
    :param cwd: Directory to run the command from
    :param print_output: If True, prints stdout/stderr as they come
    :param raise_on_error: If True, raises RuntimeError on non-zero exit code
    :return: (exit_code, stdout, stderr)

    This function:
      - Prints the command being executed
      - Times how long it takes to run
      - Prints output (stdout/stderr) if print_output=True
      - Raises an error if the command fails and raise_on_error=True
    """
    
    if isinstance(cmd, str):
        cmd_list = cmd.split()
    else:
        cmd_list = cmd

    print(f"\n[RUN] Command: {' '.join(cmd_list)}")
    if cwd:
        print(f"[RUN] Working Directory: {cwd}")
    else:
        print("[RUN] Working Directory: (current directory)")

    start_time = time.time()

    process = subprocess.Popen(
        cmd_list,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    out, err = process.communicate()
    exit_code = process.returncode

    end_time = time.time()
    duration = end_time - start_time
    print(f"[INFO] Command finished in {duration:.2f} seconds.")

    if print_output:
        if out.strip():
            print("[STDOUT]\n" + out.strip())
        if err.strip():
            print("[STDERR]\n" + err.strip(), file=sys.stderr)

    if raise_on_error and exit_code != 0:
        raise RuntimeError(f"Command '{cmd_list}' failed with exit code {exit_code}")

    return exit_code, out, err


def ensure_homebrew():
    """
    Checks if Homebrew is installed; if not, installs it.
    """
    try:
        run_command("brew --version", raise_on_error=False, print_output=False)
        print("[OK] Homebrew is already installed.")
    except RuntimeError:
        print("[INFO] Homebrew not found. Attempting to install...")
        run_command('/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"')
        print("[OK] Homebrew installed successfully.")
        
        os.environ["PATH"] += os.pathsep + "/usr/local/bin"
        os.environ["PATH"] += os.pathsep + "/opt/homebrew/bin"


def brew_install(package_name):
    """
    Installs a package via Homebrew if not already installed.
    """
    print(f"[INFO] Installing {package_name} via brew if not present...")
    cmd = f"brew list {package_name}"
    exit_code, _, _ = run_command(cmd, raise_on_error=False, print_output=False)
    if exit_code == 0:
        print(f"[OK] {package_name} is already installed.")
    else:
        run_command(f"brew install {package_name}")
        print(f"[OK] {package_name} installed.")


def check_java_8():
    """
    Checks if Java 8 is installed. If not, attempts to install Temurin 8 via brew cask.
    """
    print("[INFO] Checking Java version(s) ...")
    exit_code, out, err = run_command("java -version", raise_on_error=False)
    
    if "1.8" in err or "1.8" in out:
        print("[OK] Java 8 appears to be available.")
        return True

    print("[WARN] Java 8 not found, installing Temurin8 via Homebrew cask ...")
    run_command("brew tap homebrew/cask-versions", raise_on_error=False)
    run_command("brew install --cask temurin8")
    
    exit_code, out, err = run_command("java -version", raise_on_error=False)
    if "1.8" in err or "1.8" in out:
        print("[OK] Java 8 is now installed via Temurin.")
        return True

    print("[ERROR] Could not verify Java 8 installation. Please check manually.")
    return False


def install_maven_3_6_3(target_dir="/usr/local/apache-maven-3.6.3"):
    """
    Installs Maven 3.6.3 if not found. We specifically need that version.
    Installs to /usr/local/apache-maven-3.6.3 by default (adjust if desired).
    """
    print("[INFO] Checking if Maven 3.6.3 is installed ...")
    exit_code, out, err = run_command("mvn -version", raise_on_error=False)
    if exit_code == 0 and "3.6.3" in out:
        print("[OK] Maven 3.6.3 is already installed & in PATH.")
        return

    print("[INFO] Downloading Apache Maven 3.6.3 ...")
    maven_url = "https://archive.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.zip"
    maven_zip = "apache-maven-3.6.3-bin.zip"
    if not os.path.exists(maven_zip):
        run_command(f"wget {maven_url} -O {maven_zip}")

    print("[INFO] Extracting Maven 3.6.3 ...")
    if not os.path.exists("apache-maven-3.6.3"):
        run_command(f"unzip -o {maven_zip}")

    
    if os.path.exists(target_dir):
        print(f"[WARN] {target_dir} already exists, skipping move.")
    else:
        run_command(f"sudo mv apache-maven-3.6.3 {target_dir}")

    print("[INFO] Adding Maven 3.6.3 bin to your PATH. (Temporary for this session)")
    new_path = os.path.join(target_dir, "bin")
    os.environ["PATH"] = f"{new_path}:{os.environ['PATH']}"

    
    exit_code, out, err = run_command("mvn -version", raise_on_error=False)
    if exit_code == 0 and "3.6.3" in out:
        print("[OK] Maven 3.6.3 installed and verified.")
    else:
        print("[ERROR] Could not verify Maven 3.6.3. Please check your environment manually.")


def clone_repo(repo_url, dest_dir):
    """
    Generic function to clone a Git repo (used for JanusGraph).
    If already cloned, it skips.
    """
    if os.path.exists(dest_dir):
        print(f"[OK] Directory '{dest_dir}' already exists, skipping clone.")
    else:
        run_command(f"git clone {repo_url} {dest_dir}")


def install_lazydocker():
    """
    Installs lazydocker via Homebrew.
    """
    print("[INFO] Installing lazydocker (terminal UI for Docker)...")
    brew_install("lazydocker")
    print("[OK] lazydocker installation complete. Remember you still need Docker Engine running.")


def install_docker_cli_tools():
    """
    Installs docker-credential-helper, docker-compose, and Postman via brew.
    """
    brew_install("docker-credential-helper")
    brew_install("docker-compose")
    print("[INFO] Installing Postman (via cask) ...")
    run_command("brew install --cask postman", raise_on_error=False)


def build_keycloak():
    """
    Downloads the keycloak-15.0.2.1.zip from S3 and unzips it into ~/.m2/repository/org/keycloak.
    """
    print("[INFO] Building Keycloak 15.0.2.1 locally ...")

    keycloak_dir = os.path.expanduser("~/.m2/repository/org/keycloak")
    if not os.path.exists(keycloak_dir):
        os.makedirs(keycloak_dir, exist_ok=True)

    zip_name = "keycloak-15.0.2.1.zip"
    keycloak_url = "https://atlan-public.s3.eu-west-1.amazonaws.com/artifact/keycloak-15.0.2.1.zip"

    if not os.path.exists(zip_name):
        run_command(f"wget {keycloak_url} -O {zip_name}")

    run_command(f"unzip -o {zip_name} -d {keycloak_dir}")
    print("[OK] Keycloak 15.0.2.1 setup is done.")


def build_janusgraph():
    """
    Clones the atlan-janusgraph repo at branch=atlan-v0.6.0
    and runs 'mvn clean install ...' to install in the local m2 repo.

    NOTE: This version clones atlan-janusgraph as a sibling
    to the 'atlas-metastore' folder in which this script resides.
    """
    print("[INFO] Cloning & building atlan-janusgraph ...")

    
    repo_url = "https://github.com/atlanhq/atlan-janusgraph.git"

    
    repo_parent_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
    
    repo_dir = os.path.join(repo_parent_dir, "atlan-janusgraph")

    
    clone_repo(repo_url, repo_dir)

    
    run_command("git fetch atlan-v0.6.0", cwd=repo_dir)
    run_command("git checkout atlan-v0.6.0", cwd=repo_dir)

    
    build_cmd = [
        "mvn", "clean", "install",
        "-Dgpg.skip=true",
        "-Pjanusgraph-release",
        "-DskipTests=true",
        "-Dcheckstyle.skip=true",
        "-Drat.skip=true",
        "-Dit.skip=true",
        "-Denforcer.skip=true",
        "--batch-mode",
        "--also-make"
    ]
    print("[INFO] Building JanusGraph. This might take a while...")
    run_command(build_cmd, cwd=repo_dir)

    print("[OK] JanusGraph built successfully.")


def build_atlas():
    """
    Attempts to build the atlas-metastore with skip tests and dist profile.
    """
    print("[INFO] Building atlas-metastore ...")

    
    repo_dir = os.path.dirname(os.path.abspath(__file__))

    
    build_cmd = [
        "mvn", "clean",
        "-DskipTests", "package",
        "-Pdist",
        "-Drat.skip=true",
        "-DskipEnunciate=true"
    ]
    try:
        run_command(build_cmd, cwd=repo_dir)
        print("[OK] Built atlas-metastore with the dist profile.")
    except Exception:
        print("[WARN] Simple build command failed or partial. Attempting fallback build ...")
        fallback_cmd = [
            "mvn",
            "-pl",
            "!test-tools,!addons/hdfs-model,!addons/hive-bridge,!addons/hive-bridge-shim,"
            "!addons/falcon-bridge-shim,!addons/falcon-bridge,!addons/sqoop-bridge,!addons/sqoop-bridge-shim,"
            "!addons/hbase-bridge,!addons/hbase-bridge-shim,!addons/hbase-testing-util,"
            "!addons/kafka-bridge,!addons/impala-hook-api,!addons/impala-bridge-shim,!addons/impala-bridge",
            "-Dmaven.test.skip",
            "-DskipTests",
            "-Drat.skip=true",
            "-DskipEnunciate=true",
            "package",
            "-Pdist"
        ]
        run_command(fallback_cmd, cwd=repo_dir)
        print("[OK] Fallback build command succeeded.")


def main():
    if platform.system().lower() != "darwin":
        print("[ERROR] This script is designed for macOS only.")
        sys.exit(1)

    print("=======================================================")
    print(" Automating Apache Atlas (Metastore) Local Setup Steps ")
    print("  (Installing lazydocker instead of Rancher Desktop)   ")
    print("=======================================================")

    
    ensure_homebrew()

    
    brew_install("wget")
    brew_install("unzip")

    
    check_java_8()

    
    install_maven_3_6_3()

    
    install_docker_cli_tools()

    
    ans = input("\nDo you already have a Docker client? (yes/no): ").strip().lower()
    if ans in ("yes", "y"):
        print("[INFO] Skipping lazydocker installation.")
    else:
        install_lazydocker()

    
    build_keycloak()

    
    build_janusgraph()

    
    build_atlas()

    
    print("\n=======================================================")
    print(" All core dependencies should now be installed.")
    print(" Next Steps:")
    print(" 1. Ensure you have Docker Engine running (Docker Desktop, Colima, etc.).")
    print(" 2. Place the 'deploy' folder into your atlas-metastore root (if not already).")
    print(" 3. Start your containers with docker-compose in ~/atlas (or your chosen dir).")
    print(" 4. In IntelliJ, create the Run Configuration for 'org.apache.atlas.Atlas' (Java 8).")
    print(" 5. Use the VM options from the instructions to point to deploy/conf, logs, etc.")
    print(" 6. Run/Debug Atlas in IntelliJ, which should be on http://localhost:21000.")
    print("=======================================================")
    print("[INFO] Setup script completed. Some manual steps remain as described above.")


if __name__ == "__main__":
    main()
