import { createCoordinatorHandler } from '@vaultsync/next/server';
import { InMemoryCoordinator } from '@vaultsync/node';

// Instantiate mock in-memory coordinator for Next.js API Routes
const coordinator = new InMemoryCoordinator();
const handler = createCoordinatorHandler(coordinator);

export const GET = handler.GET;
export const POST = handler.POST;
export const PUT = handler.POST;
export const DELETE = handler.POST;
export const PATCH = handler.POST;
export const OPTIONS = handler.GET;
export const HEAD = handler.GET;
