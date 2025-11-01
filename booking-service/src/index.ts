import { Kafka } from 'kafkajs'
import { Prisma, PrismaClient } from '@prisma/client'

const prisma = new PrismaClient()

const kafka = new Kafka({
	clientId: 'booking-service',
	brokers: ['kafka:29092'],
})

const consumer = kafka.consumer({ groupId: 'booking-group' })

const start = async () => {
	try {
		await consumer.connect()
		console.log('Consumer connected')
		await consumer.subscribe({ topic: 'booking.requests', fromBeginning: true })
		console.log('Subscribed to topic: booking.requests')
	} catch (error) {
		console.error('Failed to connect or subscribe:', error)
		process.exit(1)
	}

	await consumer.run({
		eachMessage: async ({ message }) => {
			if (!message.value) return

			try {
				const booking = JSON.parse(message.value.toString())

				const currentBooking = await prisma.booking.findUnique({
					where: { id: booking.id },
				})
				if (!currentBooking) {
					console.error(`${booking.id} not found`)
					return
				}
				if (currentBooking.status !== 'CREATED') {
					console.log(
						`${booking.id} processed: ${currentBooking.status}`
					)
					return
				}

				await prisma.booking.update({
					where: { id: booking.id },
					data: { status: 'CHECKING_AVAILABILITY' },
				})

				const finalStatus = await prisma.$transaction(
					async (tx: Prisma.TransactionClient) => {
						await tx.booking.update({
							where: { id: booking.id },
							data: { status: 'CHECKING_AVAILABILITY' },
						})

						const bookingTime = new Date(booking.datetime)
						const timeWindowStart = new Date(
							bookingTime.getTime() - 2 * 60 * 60 * 1000
						)
						const timeWindowEnd = new Date(
							bookingTime.getTime() + 2 * 60 * 60 * 1000
						)
						const conflictingBooking = await tx.booking.findFirst({
							where: {
								restaurantName: booking.restaurantName,
								datetime: {
									gte: timeWindowStart,
									lte: timeWindowEnd,
								},
								status: 'CONFIRMED',
								id: { not: booking.id },
							},
						})

						return conflictingBooking ? 'REJECTED' : 'CONFIRMED'
					}
				)

				await prisma.booking.update({
					where: { id: booking.id },
					data: { status: finalStatus },
				})

				console.log(`${booking.id} booking change status on ${finalStatus}`)
			} catch (error) {
				console.error('Error processing message:', error)
			}
		},
	})
}

const gracefulShutdown = async () => {
	console.log('Shutting down consumer...')

	try {
		await consumer.disconnect()
		console.log('Consumer disconnected')
		process.exit(0)
	} catch (error) {
		console.error('Error during shutdown:', error)
		process.exit(1)
	}
}

process.on('SIGINT', gracefulShutdown)
process.on('SIGTERM', gracefulShutdown)

start().catch(error => {
	console.error('Failed to start booking service:', error)
	process.exit(1)
})
