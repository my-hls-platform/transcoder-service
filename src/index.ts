import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge'
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg'
import { SQSEvent } from 'aws-lambda'
import { error } from 'console'
import ffmpeg from 'fluent-ffmpeg'
import * as fs from 'fs'
import * as path from 'path'
import { pipeline } from 'stream/promises'

ffmpeg.setFfmpegPath(ffmpegInstaller.path)

const s3Client = new S3Client({})
const eventBridgeClient = new EventBridgeClient()

export const handler = async (event: SQSEvent) => {
	console.log('Received SQS Event:', JSON.stringify(event, null, 2))

	const sqsMessage = event.Records[0].body
	const eventBridgePayload = JSON.parse(sqsMessage)

	const sourceBucket = eventBridgePayload.detail.bucket.name
	const objectKey = eventBridgePayload.detail.object.key

	const inputFilePath = path.join('/tmp', objectKey)
	const outputDir = path.join('/tmp', `output_${Date.now()}`)
	const outputFileName = 'playlist.m3u8'
	const outputFilePath = path.join(outputDir, outputFileName)

	try {
		if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true })
		console.log(`Downloading ${objectKey} from ${sourceBucket}...`)
		const getObjectRes = await s3Client.send(
			new GetObjectCommand({ Bucket: sourceBucket, Key: objectKey }),
		)
		await pipeline(
			getObjectRes.Body as NodeJS.ReadableStream,
			fs.createWriteStream(inputFilePath),
		)

		console.log('Starting FFmpeg transcoding...')
		await new Promise((resolve, reject) => {
			ffmpeg(inputFilePath)
				.outputOptions([
					'-profile:v baseline',
					'-level 3.0',
					'-start_number 0',
					'-hls_time 10',
					'-hls_list_size 0',
					'-f hls',
				])
				.output(outputFilePath)
				.on('end', resolve)
				.on('error', reject)
				.run()
		})
		console.log('Transcoding finished!')

		const processedBucket = process.env.PROCESSED_BUCKET_NAME
		if (!processedBucket) throw new Error('PROCESSED_BUCKET_NAME is not set')

		const filesToUpload = fs.readdirSync(outputDir)
		const hlsFolderKey = objectKey.split('.')[0]

		for (const file of filesToUpload) {
			const filePath = path.join(outputDir, file)
			const fileStream = fs.createReadStream(filePath)
			const s3Key = `${hlsFolderKey}/${file}`
			await s3Client.send(
				new PutObjectCommand({
					Bucket: processedBucket,
					Key: s3Key,
					Body: fileStream,
					ContentType: file.endsWith('.m3u8')
						? 'application/vnd.apple.mpegurl'
						: 'video/MP2T',
				}),
			)
			console.log(`Uploaded ${s3Key}`)
		}

		const masterPlaylistUrl = `${hlsFolderKey}/${outputFileName}`

		await eventBridgeClient.send(
			new PutEventsCommand({
				Entries: [
					{
						Source: 'hls-platform.transcoder',
						DetailType: 'Video.Transcoded',
						Detail: JSON.stringify({
							originalKey: objectKey,
							hlsUrl: masterPlaylistUrl,
							status: 'READY',
						}),
						EventBusName: 'default',
					},
				],
			}),
		)
		console.log('EventBridge success event sent.')
		return { statusCode: 200, body: 'Success' }
	} catch {
		console.error('Error processing video:', error)
		throw error
	} finally {
		if (fs.existsSync(inputFilePath)) fs.unlinkSync(inputFilePath)
		if (fs.existsSync(outputDir)) fs.rmSync(outputDir, { recursive: true, force: true })
	}
}
