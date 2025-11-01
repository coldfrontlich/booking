import express from 'express'
import { PrismaClient } from '@prisma/client'
import { Kafka, Partitioners } from 'kafkajs'

const app = express()
const prisma = new PrismaClient()

const kafka = new Kafka({
	clientId: 'api-service',
	brokers: ['kafka:29092'],
})
const producer = kafka.producer({
	createPartitioner: Partitioners.DefaultPartitioner,
})

app.use(express.json())

interface validateData {
	restaurantName: string
	datetime: Date
	guests: number
}
const validateBookingRequest = ({
	restaurantName,
	datetime,
	guests,
}: validateData) => {
	const errors = []

	if (
		!restaurantName ||
		typeof restaurantName !== 'string' ||
		restaurantName.trim().length === 0
	) {
		errors.push('Restaurant name is required and must be a non-empty string')
	}

	if (!datetime || isNaN(new Date(datetime).getTime())) {
		errors.push('Valid datetime is required')
	} else if (new Date(datetime) < new Date()) {
		errors.push('Booking datetime must be in the future')
	}

	if (!guests || !Number.isInteger(guests) || guests <= 0 || guests > 15) {
		errors.push('Guests must be an integer between 1 and 15')
	}

	return errors
}

app.post('/bookings', async (req, res) => {
	try {
		const { restaurantName, datetime, guests } = req.body

		const validationErrors = validateBookingRequest({
			restaurantName,
			datetime,
			guests,
		})
		if (validationErrors.length > 0) {
			return res.status(400).json({
				error: 'Validation failed',
				details: validationErrors,
			})
		}

		const booking = await prisma.booking.create({
			data: { restaurantName, datetime: new Date(datetime), guests },
		})

		await producer.send({
			topic: 'booking.requests',
			messages: [{ value: JSON.stringify(booking) }],
		})

		return res.status(201).json(booking)
	} catch (error) {
		return res.status(500).json({ error: 'Error while creating booking' })
	}
})

app.get('/booking/:id', async (req, res) => {
	try {
		const { id } = req.params
		const booking = await prisma.booking.findUnique({ where: { id } })
		if (!booking) {
			return res.status(404).json({ error: 'Not found booking' })
		}
		return res.json(booking)
	} catch (error) {
		return res.status(500).json({ error: 'Error while fetching booking' })
	}
})

const start = async () => {
	try {
		await producer.connect()
		console.log('Producer connected successfully')
	} catch (error) {
		console.error('Failed to connect producer:', error)
		process.exit(1)
	}

	const server = app.listen(3000, () => {
		console.log('listening on port 3000')
	})

	const gracefulShutdown = async () => {
		console.log('Closing server...')

		server.close(async () => {
			try {
				await producer.disconnect()
				console.log('Producer disconnected')
				process.exit(0)
			} catch (error) {
				console.error('Error during producer disconnect:', error)
				process.exit(1)
			}
		})

		setTimeout(() => {
			console.error('Shutdown after timeout')
			process.exit(1)
		}, 10000)
	}

	process.on('SIGTERM', gracefulShutdown)
	process.on('SIGINT', gracefulShutdown)
}

start()
