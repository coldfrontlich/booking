-- CreateTable
CREATE TABLE "Booking" (
    "id" TEXT NOT NULL,
    "restaurantName" TEXT NOT NULL,
    "datetime" TIMESTAMP(3) NOT NULL,
    "guests" INTEGER NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'CREATED',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Booking_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "Booking_restaurantName_datetime_status_idx" ON "Booking"("restaurantName", "datetime", "status");

-- CreateIndex
CREATE INDEX "Booking_status_idx" ON "Booking"("status");

-- CreateIndex
CREATE INDEX "Booking_createdAt_idx" ON "Booking"("createdAt");
