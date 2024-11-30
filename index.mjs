import { BedrockRuntimeClient, InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const pdfParse = require('pdf-parse');

const s3Client = new S3Client();
const bedrockClient = new BedrockRuntimeClient({ region: "eu-central-1" });

// Bekleme fonksiyonu
const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Metni parçalara bölen fonksiyon
function splitTextIntoChunks(text, chunkSize = 80000) {
    const chunks = [];
    let startIndex = 0;
    
    while (startIndex < text.length) {
        let endIndex = startIndex + chunkSize;
        
        // Cümle sonunda bölmeye çalış
        if (endIndex < text.length) {
            const nextPeriod = text.indexOf('.', endIndex - 100);
            if (nextPeriod !== -1 && nextPeriod - endIndex < 100) {
                endIndex = nextPeriod + 1;
            }
        }
        
        chunks.push(text.slice(startIndex, endIndex));
        startIndex = endIndex;
    }
    
    return chunks;
}

// Tek bir parça için özet oluşturan fonksiyon - retry mekanizması eklendi
async function generateSummaryForChunk(text, chunkIndex, totalChunks, retryCount = 0) {
    try {
        const input = {
            modelId: "anthropic.claude-3-5-sonnet-20240620-v1:0",
            contentType: "application/json",
            accept: "application/json",
            body: JSON.stringify({
                anthropic_version: "bedrock-2023-05-31",
                max_tokens: 4000,
                messages: [
                    {
                        role: "user",
                        content: `Sen bir kitap özeti çıkaran asistansın. Bu metin kitabın ${totalChunks} parçasından ${chunkIndex + 1}. parçası. 
                        Lütfen bu bölümün özetini Türkçe olarak hazırla.

                        Özette şunlara dikkat et:
                        - Bu bölümdeki ana temalar ve konular
                        - Önemli olaylar ve argümanlar
                        - Anahtar fikirler ve kavramlar

                        Metin:
                        ${text}

                        Not: Bu bir ara özet olduğu için, diğer bölümlerle bağlantı kurmaya çalışma, sadece bu bölümdeki bilgilere odaklan.`
                    }
                ],
                temperature: 0.7,
                top_p: 0.9,
                top_k: 250
            })
        };

        const command = new InvokeModelCommand(input);
        const response = await bedrockClient.send(command);
        const responseBody = JSON.parse(new TextDecoder().decode(response.body));
        return responseBody.content[0].text;
    } catch (error) {
        if (error.message.includes('Too many requests') && retryCount < 3) {
            console.log(`Rate limit hit for chunk ${chunkIndex + 1}, waiting before retry...`);
            await wait(5000 * (retryCount + 1)); // Her retry'da daha uzun bekle
            return generateSummaryForChunk(text, chunkIndex, totalChunks, retryCount + 1);
        }
        throw error;
    }
}

// Ana özeti oluşturan fonksiyon
async function generateFinalSummary(summaries) {
    const input = {
        modelId: "anthropic.claude-3-5-sonnet-20240620-v1:0",
        contentType: "application/json",
        accept: "application/json",
        body: JSON.stringify({
            anthropic_version: "bedrock-2023-05-31",
            max_tokens: 4000,
            messages: [
                {
                    role: "user",
                    content: `Sen bir kitap özeti çıkaran asistansın. Aşağıda kitabın farklı bölümlerinin özetleri var. 
                    Bu özetleri kullanarak kitabın genel bir özetini Türkçe olarak hazırla.

                    Bölüm özetleri:
                    ${summaries.join('\n\n--- Yeni Bölüm ---\n\n')}

                    Lütfen:
                    1. Kitabın genel bir özetini çıkar
                    2. Ana temaları ve konuları belirt
                    3. Önemli argümanları ve mesajları özetle
                    4. Varsa yazarın temel görüşlerini ve sonuçları belirt`
                }
            ],
            temperature: 0.7,
            top_p: 0.9,
            top_k: 250
        })
    };

    const command = new InvokeModelCommand(input);
    const response = await bedrockClient.send(command);
    const responseBody = JSON.parse(new TextDecoder().decode(response.body));
    return responseBody.content[0].text;
}

// Sıralı işleme fonksiyonu
async function processChunksSequentially(chunks) {
    const summaries = [];
    
    for (let i = 0; i < chunks.length; i++) {
        console.log(`Processing chunk ${i + 1}/${chunks.length}`);
        const summary = await generateSummaryForChunk(chunks[i], i, chunks.length);
        summaries.push(summary);
        
        // Her chunk arasında 3 saniye bekle
        if (i < chunks.length - 1) {
            console.log('Waiting before processing next chunk...');
            await wait(3000);
        }
    }
    
    return summaries;
}

export const handler = async (event) => {
    try {
        console.log('Event:', JSON.stringify(event));

        const bucket = event.Records[0].s3.bucket.name;
        const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));

        // PDF'i al ve metne çevir
        const response = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        const pdfBuffer = Buffer.from(await response.Body.transformToByteArray());
        const data = await pdfParse(pdfBuffer);
        const fullText = data.text;

        console.log('Full text length:', fullText.length);

        // Metni parçalara böl
        const chunks = splitTextIntoChunks(fullText);
        console.log(`Text split into ${chunks.length} chunks`);

        // Sıralı işleme ile özetleri oluştur
        console.log('Starting sequential processing of chunks');
        const chunkSummaries = await processChunksSequentially(chunks);
        console.log('All chunk summaries generated');

        // Final özet öncesi bekle
        await wait(3000);
        console.log('Generating final summary');
        const finalSummary = await generateFinalSummary(chunkSummaries);

        // Özeti S3'e kaydet
        const summaryKey = key.replace('pdfs/', 'summaries/').replace('.pdf', '.txt');
        await s3Client.send(new PutObjectCommand({
            Bucket: bucket,
            Key: summaryKey,
            Body: finalSummary,
            ContentType: 'text/plain'
        }));

        // Ara özetleri de kaydet
        const detailedSummaryKey = key.replace('pdfs/', 'summaries/detailed_').replace('.pdf', '.txt');
        const detailedSummary = chunkSummaries.join('\n\n=== YENİ BÖLÜM ===\n\n');
        await s3Client.send(new PutObjectCommand({
            Bucket: bucket,
            Key: detailedSummaryKey,
            Body: detailedSummary,
            ContentType: 'text/plain'
        }));

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'PDF successfully processed and summaries created',
                summaryKey: summaryKey,
                detailedSummaryKey: detailedSummaryKey
            })
        };
    } catch (error) {
        console.error('Error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                error: 'Failed to process PDF',
                details: error.message
            })
        };
    }
};