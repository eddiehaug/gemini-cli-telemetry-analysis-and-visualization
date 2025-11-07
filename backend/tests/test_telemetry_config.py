#!/usr/bin/env python3
"""
Quick test to verify Gemini CLI telemetry configuration.
Run this to check if telemetry is configured correctly.
"""
import json
import os
import subprocess
import sys

def test_telemetry_config():
    """Test Gemini CLI telemetry configuration."""
    print("=" * 60)
    print("Gemini CLI Telemetry Configuration Test")
    print("=" * 60)

    # 1. Check settings.json
    settings_path = os.path.expanduser("~/.gemini/settings.json")
    print(f"\n1. Checking settings.json: {settings_path}")

    try:
        with open(settings_path, 'r') as f:
            settings = json.load(f)

        telemetry = settings.get("telemetry", {})
        env_vars = settings.get("env", {})

        print(f"   Telemetry enabled: {telemetry.get('enabled', False)}")
        print(f"   Telemetry target: {telemetry.get('target', 'N/A')}")
        print(f"   Log prompts: {telemetry.get('logPrompts', False)}")
        print(f"   GOOGLE_CLOUD_PROJECT: {env_vars.get('GOOGLE_CLOUD_PROJECT', 'NOT SET')}")
        print(f"   OTLP_GOOGLE_CLOUD_PROJECT: {env_vars.get('OTLP_GOOGLE_CLOUD_PROJECT', 'NOT SET')}")

        inference_project = env_vars.get('GOOGLE_CLOUD_PROJECT')
        telemetry_project = env_vars.get('OTLP_GOOGLE_CLOUD_PROJECT')

        if not telemetry.get('enabled'):
            print("   ❌ ERROR: Telemetry is not enabled!")
            return False

        if not inference_project:
            print("   ❌ ERROR: GOOGLE_CLOUD_PROJECT not set!")
            return False

        print("   ✅ Configuration looks good")

    except FileNotFoundError:
        print(f"   ❌ ERROR: Settings file not found!")
        return False
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        return False

    # 2. Check gcloud configurations
    print("\n2. Checking gcloud configurations")

    try:
        result = subprocess.run(
            ["gcloud", "config", "configurations", "list", "--format=value(name)"],
            capture_output=True,
            text=True,
            timeout=10
        )

        configs = [c.strip() for c in result.stdout.strip().split('\n') if c.strip()]
        print(f"   Found {len(configs)} configurations: {', '.join(configs)}")

        # Check for expected configurations
        if telemetry_project:
            telemetry_config = f"telemetry-{telemetry_project.split('-')[-1]}" if '-' in telemetry_project else f"telemetry-{telemetry_project}"
            gemini_config = f"gemini-cli-{inference_project.split('-')[-1]}" if '-' in inference_project else f"gemini-cli-{inference_project}"

            has_telemetry = any(telemetry_project[:20] in c for c in configs)
            has_gemini = any(inference_project[:20] in c for c in configs)

            if has_telemetry:
                print(f"   ✅ Found telemetry configuration")
            else:
                print(f"   ⚠️  Warning: Telemetry configuration not found")

            if has_gemini:
                print(f"   ✅ Found Gemini CLI configuration")
            else:
                print(f"   ⚠️  Warning: Gemini CLI configuration not found")
        else:
            print(f"   ℹ️  Single-project setup (no separate telemetry project)")

    except Exception as e:
        print(f"   ⚠️  Could not check configurations: {e}")

    # 3. Test headless mode with environment variables
    print("\n3. Testing headless mode environment variable passing")

    env = {
        **os.environ,
        "GOOGLE_CLOUD_PROJECT": inference_project,
    }

    if telemetry_project and telemetry_project != inference_project:
        env["OTLP_GOOGLE_CLOUD_PROJECT"] = telemetry_project
        print(f"   Setting GOOGLE_CLOUD_PROJECT={inference_project}")
        print(f"   Setting OTLP_GOOGLE_CLOUD_PROJECT={telemetry_project}")
    else:
        print(f"   Setting GOOGLE_CLOUD_PROJECT={inference_project}")

    try:
        print("\n4. Running test Gemini CLI command...")
        print(f"   Command: gemini --prompt 'Test' --model gemini-2.5-flash --output-format json")

        result = subprocess.run(
            ["gemini", "--prompt", "What is 2+2?", "--model", "gemini-2.5-flash", "--output-format", "json"],
            capture_output=True,
            text=True,
            timeout=30,
            env=env
        )

        if result.returncode == 0:
            print(f"   ✅ Command executed successfully")
            print(f"   Response length: {len(result.stdout)} characters")
        else:
            print(f"   ❌ Command failed with exit code {result.returncode}")
            print(f"   Error: {result.stderr[:200]}")
            return False

    except subprocess.TimeoutExpired:
        print(f"   ❌ Command timed out")
        return False
    except Exception as e:
        print(f"   ❌ Error running command: {e}")
        return False

    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Inference project: {inference_project}")
    print(f"  Telemetry project: {telemetry_project or inference_project}")
    print(f"  Logs should appear in: {telemetry_project or inference_project}")
    print("=" * 60)

    return True

if __name__ == "__main__":
    success = test_telemetry_config()
    sys.exit(0 if success else 1)
