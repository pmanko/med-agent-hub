# Setting Up LM Studio for Local Development

LM Studio provides a convenient way to run LLMs locally with an OpenAI-compatible API, perfect for developing and testing A2A agents without cloud dependencies.

## Why LM Studio?

- **Free & Local**: Run models on your own hardware
- **OpenAI Compatible**: Works seamlessly with our agents
- **On-Demand Loading**: Automatically loads models as needed (v0.3.5+)
- **GPU Accelerated**: Supports NVIDIA, AMD, and Apple Silicon

## Prerequisites

- **LM Studio v0.3.5 or newer** - Required for on-demand model loading
- **Hardware**: 8GB+ RAM (16GB recommended for larger models)
- **Disk Space**: 5-10GB per model

## Installation & Setup

### 1. Install LM Studio

Download from [lmstudio.ai](https://lmstudio.ai) for your platform:
- Windows, macOS, or Linux
- Verify version is 0.3.5+ for best experience

### 2. Configure the Local API Server

#### Option A: Developer Screen (Recommended)
1. Open LM Studio
2. Go to the Developer screen (Local Server)
3. Download the following models:
   - Router/General: `meta-llama-3.1-8b-instruct`
   - Medical: `medgemma-4b-it-mlx`
   - Clinical: `gemma-3-1b-it`
4. Start the local server (keep default `http://localhost:1234`)
5. Load models as needed while developing (LM Studio can live-load on demand)

#### Option B: Headless Mode (Alternative)
```bash
# Start LM Studio server in headless mode
lms server start --port 1234

# The API will be available at http://localhost:1234
```

Reference: [LM Studio Headless Documentation](https://lmstudio.ai/docs/app/api/headless)

### 3. Configure A2A Agents

Create your `.env` file with LM Studio as the LLM provider:

```env
# Essential Configuration
LLM_BASE_URL=http://localhost:1234  # Your LM Studio server

# Model selection (match LM Studio)
ORCHESTRATOR_MODEL=meta-llama-3.1-8b-instruct
MED_MODEL=medgemma-4b-it-mlx
CLINICAL_RESEARCH_MODEL=gemma-3-1b-it

# Optional: Use Gemini for orchestration instead of local LLM
# ORCHESTRATOR_PROVIDER=gemini
# GEMINI_API_KEY=your-api-key-here
# ORCHESTRATOR_MODEL=gemini-1.5-flash
```

## Model Selection

### Recommended Models for Each Agent

| Agent | Recommended Model | Size | Purpose |
|-------|------------------|------|---------|
| **Router** | Llama 3 8B Instruct | 4.5GB | Orchestration & routing |
| **MedGemma** | MedGemma 2B | 1.5GB | Medical Q&A |
| **Clinical** | Gemma 2 9B | 5.5GB | Query generation |

### Downloading Models

1. In LM Studio, go to the "Discover" tab
2. Search for the model name
3. Select the appropriate quantization (GGUF format)
4. Click "Download"

**Quantization Guide**:
- `Q4_K_M`: Balanced quality/performance (recommended)
- `Q5_K_M`: Better quality, more memory
- `Q8_0`: Near full precision, highest memory

## Performance Optimization

### GPU Acceleration

#### NVIDIA GPUs
1. Install CUDA 12.x
2. In LM Studio settings:
   - Enable "GPU Offload"
   - Set "GPU Layers" to maximum
   - Enable "Flash Attention" if supported

#### Apple Silicon
- Automatically uses Metal Performance Shaders
- No additional configuration needed

#### AMD GPUs
- ROCm support varies by model
- Check LM Studio documentation for compatibility

### Memory Management

```env
# In LM Studio settings or model config:
context_length: 4096      # Reduce for lower memory usage
gpu_layers: -1            # Use all available GPU layers
batch_size: 512          # Adjust based on available RAM
```

## On-Demand Model Loading

LM Studio v0.3.5+ supports automatic model loading:

1. **Enable in Settings**:
   - Settings → Advanced → "Enable on-demand loading"
   
2. **Configure Multiple Models**:
   ```env
   # Each agent can use different models with auto-switching
   GENERAL_MODEL=llama-3-8b-instruct
   MED_MODEL=medgemma-2
   ```

3. **How it Works**:
   - First request to a model triggers download/load
   - Model stays loaded until memory pressure
   - Automatic switching between models

Reference: [On-Demand Loading Guide](https://lmstudio.ai/blog/lmstudio-v0.3.5#on-demand-model-loading)

## Testing Your Setup

### 1. Verify LM Studio is Running

```bash
# Test the API endpoint
curl http://localhost:1234/v1/models

# Should return list of available models
```

### 2. Start A2A Agents

```bash
# Launch all agents
python launch_a2a_agents.py

# Or test individually
python launch_a2a_agents.py test
```

### 3. Test with Sample Query

```python
from a2a.client import AgentClient
import asyncio

async def test():
    client = AgentClient("http://localhost:9100")
    result = await client.invoke_skill(
        "route_query",
        query="What are symptoms of diabetes?"
    )
    print(result)

asyncio.run(test())
```

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| **Connection refused** | Ensure LM Studio server is running on correct port |
| **Model not found** | Download model in LM Studio first |
| **Slow responses** | Enable GPU offloading, reduce context length |
| **Out of memory** | Use smaller quantization (Q4_K_M), reduce batch size |
| **Agent timeout** | Increase `CHAT_TIMEOUT_SECONDS` in .env |

### Debug Mode

Enable verbose logging to troubleshoot:

```bash
# Start LM Studio with debug output
lms server start --verbose --port 1234

# Set agent logging to DEBUG
export LOG_LEVEL=DEBUG
python launch_a2a_agents.py
```

## Advanced Configuration

### Running Multiple Models Simultaneously

For better performance with multiple agents:

```bash
# Terminal 1: General model server
lms server start --port 1234 --model llama-3-8b

# Terminal 2: Medical model server  
lms server start --port 1235 --model medgemma-2

# Update .env to use different ports
LLM_BASE_URL=http://localhost:1234
MED_LLM_BASE_URL=http://localhost:1235
```

### Remote Access

To access LM Studio from other machines:

```bash
# Bind to all interfaces (security warning!)
lms server start --host 0.0.0.0 --port 1234

# Better: Use SSH tunneling
ssh -L 1234:localhost:1234 your-dev-machine
```

## Best Practices

1. **Model Selection**: Choose models that fit in your GPU memory
2. **Quantization**: Start with Q4_K_M for best balance
3. **Context Length**: Keep at 4096 or lower for faster responses
4. **Batch Processing**: Adjust batch_size based on concurrent users
5. **Model Caching**: Keep frequently used models loaded

## Next Steps

- [Configure the system](configuration.md) for your environment
- [Create new agents](../development/creating-agents.md) to extend functionality
- Review the [Architecture Overview](../architecture/overview.md) for system design

## Resources

- [LM Studio Documentation](https://lmstudio.ai/docs)
- [LM Studio API Reference](https://lmstudio.ai/docs/app/api)
- [Model Compatibility List](https://lmstudio.ai/models)
- [Performance Tuning Guide](https://lmstudio.ai/docs/performance)
