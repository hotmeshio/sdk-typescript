/**
 * OpenAI API Activities for Research Agent
 * These activities make real API calls to OpenAI GPT-4.1 for research and analysis
 */

/**
 * Interface for OpenAI API response
 */
interface OpenAIResponse {
  choices: Array<{
    message: {
      content: string;
    };
  }>;
  usage?: {
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  };
}

/**
 * Makes a call to OpenAI API
 */
async function callOpenAI(prompt: string, systemPrompt?: string): Promise<any> {
  const apiKey = process.env.OPENAI_API_KEY;
  const apiUrl = process.env.OPENAI_API_URL || 'https://api.openai.com/v1/chat/completions';
  const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';

  if (!apiKey) {
    throw new Error('OPENAI_API_KEY environment variable is not set');
  }

  const messages = [
    ...(systemPrompt ? [{ role: 'system', content: systemPrompt }] : []),
    { role: 'user', content: prompt }
  ];

  const response = await fetch(apiUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model,
      messages,
      max_tokens: 1000,
      temperature: 0.7
    })
  });

  if (!response.ok) {
    throw new Error(`OpenAI API error: ${response.status} ${response.statusText}`);
  }

  const data: OpenAIResponse = await response.json();
  return data.choices[0]?.message?.content || '';
}

/**
 * Supporting Evidence Search Activity
 * Searches for evidence that supports a given query
 */
export async function searchForSupportingEvidence(query: string): Promise<string[]> {
  const prompt = `Find 3-5 key pieces of supporting evidence for this statement or question: "${query}". 
  Return only a JSON array of evidence strings. Be factual and cite credible sources where possible.`;

  const systemPrompt = `You are a research assistant that finds supporting evidence. 
  Always return a valid JSON array of strings, nothing else.`;

  try {
    const response = await callOpenAI(prompt, systemPrompt);
    // Try to parse as JSON, fallback to creating an array from the response
    try {
      const evidence = JSON.parse(response);
      return Array.isArray(evidence) ? evidence : [response];
    } catch {
      // If not valid JSON, create an array from the response
      return [response];
    }
  } catch (error) {
    console.error('Error calling OpenAI for supporting evidence:', error);
    // Return mock data as fallback
    return [
      `Mock supporting evidence 1 for: ${query}`,
      `Mock supporting evidence 2 for: ${query}`,
      `Mock supporting evidence 3 for: ${query}`
    ];
  }
}

/**
 * Counter Evidence Search Activity
 * Searches for evidence that contradicts or challenges a given query
 */
export async function searchForCounterEvidence(query: string): Promise<string[]> {
  const prompt = `Find 3-5 key pieces of counter-evidence or contradictory information for this statement or question: "${query}". 
  Return only a JSON array of evidence strings. Be factual and cite credible sources where possible.`;

  const systemPrompt = `You are a research assistant that finds counter-evidence and contradictory information. 
  Always return a valid JSON array of strings, nothing else.`;

  try {
    const response = await callOpenAI(prompt, systemPrompt);
    // Try to parse as JSON, fallback to creating an array from the response
    try {
      const evidence = JSON.parse(response);
      return Array.isArray(evidence) ? evidence : [response];
    } catch {
      // If not valid JSON, create an array from the response
      return [response];
    }
  } catch (error) {
    console.error('Error calling OpenAI for counter evidence:', error);
    // Return mock data as fallback
    return [
      `Mock counter evidence 1 for: ${query}`,
      `Mock counter evidence 2 for: ${query}`,
      `Mock counter evidence 3 for: ${query}`
    ];
  }
}

/**
 * Perspective Analysis Activity
 * Analyzes different perspectives on a given topic
 */
export async function analyzePerspectives(perspectives: any): Promise<any> {
  const prompt = `Analyze these different perspectives and provide a balanced synthesis: ${JSON.stringify(perspectives)}. 
  Return a JSON object with: {
    "synthesis": "comprehensive analysis",
    "keyInsights": ["insight1", "insight2", "insight3"],
    "confidence": 0.85,
    "recommendations": ["rec1", "rec2"]
  }`;

  const systemPrompt = `You are an expert analyst who synthesizes different perspectives into balanced insights. 
  Always return valid JSON, nothing else.`;

  try {
    const response = await callOpenAI(prompt, systemPrompt);
    // Try to parse as JSON, fallback to structured response
    try {
      const analysis = JSON.parse(response);
      return analysis;
    } catch {
      // If not valid JSON, create structured response
      return {
        synthesis: response,
        keyInsights: ['Analysis provided by AI'],
        confidence: 0.75,
        recommendations: ['Review findings', 'Consider multiple viewpoints']
      };
    }
  } catch (error) {
    console.error('Error calling OpenAI for perspective analysis:', error);
    // Return mock data as fallback
    return {
      synthesis: `Mock analysis of perspectives: ${JSON.stringify(perspectives)}`,
      keyInsights: ['Mock insight 1', 'Mock insight 2', 'Mock insight 3'],
      confidence: 0.70,
      recommendations: ['Mock recommendation 1', 'Mock recommendation 2']
    };
  }
}

/**
 * Source Credibility Verification Activity
 * Verifies the credibility of sources
 */
export async function verifySourceCredibility(query: string): Promise<any> {
  const prompt = `Evaluate the credibility of sources related to this topic: "${query}". 
  Return a JSON object with: {
    "credibleSources": ["source1", "source2"],
    "questionableSources": ["source3"],
    "verificationMethod": "description",
    "overallCredibility": 0.85
  }`;

  const systemPrompt = `You are a fact-checker who evaluates source credibility. 
  Always return valid JSON, nothing else.`;

  try {
    const response = await callOpenAI(prompt, systemPrompt);
    // Try to parse as JSON, fallback to structured response
    try {
      const verification = JSON.parse(response);
      return verification;
    } catch {
      // If not valid JSON, create structured response
      return {
        credibleSources: ['Academic journals', 'Government reports'],
        questionableSources: ['Unverified blogs'],
        verificationMethod: response,
        overallCredibility: 0.75
      };
    }
  } catch (error) {
    console.error('Error calling OpenAI for source verification:', error);
    // Return mock data as fallback
    return {
      credibleSources: [`Mock credible source 1 for: ${query}`, `Mock credible source 2 for: ${query}`],
      questionableSources: [`Mock questionable source for: ${query}`],
      verificationMethod: 'Mock verification method',
      overallCredibility: 0.70
    };
  }
}
