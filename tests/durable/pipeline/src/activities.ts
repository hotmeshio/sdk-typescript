import * as fs from 'fs';
import * as path from 'path';
import OpenAI from 'openai';

/**
 * OpenAI Vision API Activities for Document Processing Pipeline
 * These activities handle PNG image processing and OpenAI Vision API calls
 */

/**
 * Interface for extracted member information
 */
interface MemberInfo {
  memberId?: string;
  address?: {
    street: string;
    city: string;
    state: string;
    zip: string;
  };
  name?: string;
  phone?: string;
  email?: string;
  emergencyContact?: {
    name: string;
    phone: string;
  };
  isPartialInfo?: boolean;
}

/**
 * Simulated cloud storage interface
 * In production, this would be replaced with actual cloud storage SDK (GCS/S3)
 */
interface CloudStorage {
  getObject(key: string): Promise<string>;
  putObject(key: string, content: string): Promise<void>;
  listObjects(prefix: string): Promise<string[]>;
}

class LocalCloudStorage implements CloudStorage {
  private baseDir: string;

  constructor(baseDir: string) {
    this.baseDir = baseDir;
  }

  async getObject(key: string): Promise<string> {
    const filePath = path.join(this.baseDir, key);
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${key}`);
    }
    return fs.readFileSync(filePath, 'base64');
  }

  async putObject(key: string, content: string): Promise<void> {
    const filePath = path.join(this.baseDir, key);
    fs.writeFileSync(filePath, Buffer.from(content, 'base64'));
  }

  async listObjects(prefix: string): Promise<string[]> {
    const dir = path.join(this.baseDir, prefix);
    if (!fs.existsSync(dir)) {
      return [];
    }
    return fs.readdirSync(dir)
      .filter(file => file.endsWith('.png'))
      .map(file => path.join(prefix, file));
  }
}

// Initialize cloud storage with fixtures directory
const storage = new LocalCloudStorage(path.join(__dirname, '..', 'fixtures'));

/**
 * Returns list of image file references from cloud storage
 * In production, this would return GCS/S3 object keys
 */
export async function loadImagePages(): Promise<string[]> {
  try {
    const imageRefs = await storage.listObjects('');
    
    if (imageRefs.length === 0) {
      throw new Error('No PNG image files found in storage');
    }
    
    return imageRefs;
  } catch (error) {
    console.error('Error listing image pages:', error);
    throw new Error('Failed to list image files from storage');
  }
}

/**
 * Makes a call to OpenAI Vision API with image content
 */
async function callOpenAIVision(prompt: string, imageContent: string): Promise<any> {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error('OpenAI API key is required. Set OPENAI_API_KEY environment variable.');
  }

  const openai = new OpenAI({
    apiKey: apiKey
  });

  try {
    const response = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        {
          role: "user",
          content: [
            { type: "text", text: prompt },
            {
              type: "image_url",
              image_url: {
                url: `data:image/png;base64,${imageContent}`,
                detail: "high"  // Use high detail for better OCR and analysis
              }
            }
          ]
        }
      ],
      max_tokens: 1000
    });

    if (!response.choices || !response.choices[0] || !response.choices[0].message) {
      throw new Error('Invalid response from OpenAI API');
    }

    return response.choices[0].message.content;
  } catch (error: any) {
    if (error.response) {
      throw new Error(`OpenAI API error: ${error.response.status} ${error.response.statusText}\n${JSON.stringify(error.response.data, null, 4)}\n`);
    }
    throw error;
  }
}

/**
 * Extracts member information from a PNG image page using OpenAI Vision API
 * Now accepts a file reference instead of content
 */
export async function extractMemberInfoFromPage(imageRef: string, pageNumber: number): Promise<MemberInfo | null> {
  try {
    // Fetch image content from storage when needed
    const imageContent = await storage.getObject(imageRef);
    
    const prompt = `
      Extract member information from this membership application page image.
      
      Required fields (if found):
      - Member ID (format: MBR-YYYY-XXX)
      - Full address (street, city, state, ZIP)
      - Name
      - Phone number
      - Email address
      - Emergency contact information (name and phone)
      
      Return ONLY a valid JSON object with this exact structure:
      {
        "memberId": "string",
        "address": {
          "street": "string",
          "city": "string", 
          "state": "string",
          "zip": "string"
        },
        "name": "string",
        "phone": "string",
        "email": "string",
        "emergencyContact": {
          "name": "string",
          "phone": "string"
        }
      }
      
      Include only fields that are found in the image. Omit any fields not found.
      Do not include any explanatory text, only return the JSON object.
    `;
    
    const response = await callOpenAIVision(prompt, imageContent);
    
    try {
      // Remove any markdown code block syntax if present
      const jsonStr = response.replace(/^```json\n?|\n?```$/g, '').trim();
      const parsedInfo = JSON.parse(jsonStr);
      
      // If no member ID found, return partial info (will be associated with previous page's member)
      if (!parsedInfo.memberId) {
        return { ...parsedInfo, isPartialInfo: true } as MemberInfo;
      }
      
      return parsedInfo as MemberInfo;
    } catch (parseError) {
      console.warn(`Failed to parse OpenAI response for page ${pageNumber}:`, response);
      console.warn('Parse error:', parseError);
      return null;
    }
  } catch (error) {
    console.error(`Error extracting member info from page ${pageNumber}:`, error);
    throw error;
  }
}

/**
 * Validates member information against the member database
 */
export async function validateMemberInfo(memberInfo: MemberInfo): Promise<boolean> {
  try {
    // Check if we have required information for validation
    if (!memberInfo.memberId) {
      return false;
    }
    
    if (!memberInfo.address) {
      return false;
    }
    
    const dbPath = path.join(__dirname, '..', 'fixtures', 'member-database.json');
    const dbContent = fs.readFileSync(dbPath, 'utf8');
    const database = JSON.parse(dbContent);
    
    // Check if member exists in database
    const member = database.members[memberInfo.memberId];
    if (!member) {
      return false;
    }
    
    // Validate address matches
    const dbAddress = member.address;
    const infoAddress = memberInfo.address;
    
    const addressMatches = (
      dbAddress.street === infoAddress.street &&
      dbAddress.city === infoAddress.city &&
      dbAddress.state === infoAddress.state &&
      dbAddress.zip === infoAddress.zip
    );
    
    if (!addressMatches) {
      return false;
    }
    
    // Check if member is active
    if (member.status !== 'active') {
      return false;
    }
    
    return true;
  } catch (error) {
    console.error('Error validating member info:', error);
    throw error;
  }
}

/**
 * Processes document approval workflow
 */
export async function processDocumentApproval(memberInfo: MemberInfo, validationResult: boolean): Promise<any> {
  try {
    const result = {
      memberId: memberInfo.memberId,
      validationResult,
      status: validationResult ? 'approved' : 'rejected',
      processedAt: new Date().toISOString(),
      nextSteps: validationResult 
        ? ['Send welcome email', 'Activate benefits', 'Generate member card']
        : ['Send rejection notice', 'Request updated information'],
      memberType: 'premium', // Mock - would be determined from database
      benefits: validationResult 
        ? ['gym_access', 'pool_access', 'personal_training']
        : []
    };
    
    // Simulate processing delay
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    return result;
  } catch (error) {
    console.error('Error processing document approval:', error);
    throw error;
  }
}

/**
 * Sends notification about processing result
 */
export async function sendNotification(result: any): Promise<void> {
  try {
    await new Promise(resolve => setTimeout(resolve, 500));
  } catch (error) {
    console.error('Error sending notification:', error);
    throw error;
  }
} 